import json
from decimal import Decimal
import time
import base64
import boto3
import stringcase

from aws_xray_sdk.core import xray_recorder  # noqa: F401
from aws_xray_sdk.core import patch_all
from boto3.dynamodb.conditions import Key
from dynamodb_json import json_util
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import Any, MultipleInvalid
from botocore.exceptions import ClientError
from dynamodb.store import put_work_order, query, get_work_order, DynamoItemNotFound
from order_capture import add_capture_data, add_capture_data_summary
from order_condition import process_condition, add_condition_data_summary
from order_offering import add_offering_data
from rpp_lib.rpc import get_pfvehicle
from utils.common import get_vin
from utils.dynamodb import remove_item
from recon_labor_status import (
    process_labor_status,
    update_labor_status,
    build_labor,
    get_overall_damage_status,
)
from order_approval_summary import process_approval_summary
from damages import LABOR_TYPES, create_isdt_key
from order_retailrecon import delete_work_order, process_retail_recon


patch_all()

RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"

ENV = Env()
SQS = boto3.resource("sqs")
QUEUE = SQS.get_queue_by_name(QueueName=ENV("QUEUE", validate=Any(str)))
DYNAMO = boto3.resource("dynamodb")
KINESIS = boto3.client("kinesis")
TABLE = ENV("WORKORDER_AM_TABLE")
# TABLE = "rpp-recon-work-order"


ACTION = {
    "approval_id": {"general": True, "name": "approval"},
    "capture_id": {"general": True, "name": "capture"},
    "certification_id": {"general": True, "name": "certification"},
    "condition_id": {"general": True, "name": "conditions"},
    "detail_id": {"general": True, "name": "detail"},
    "offering_id": {"general": False, "name": "offering", "process": add_offering_data},
    "retailrecon_id": {"general": True, "name": "retail_recon"},
    "pksk": {"general": True, "name": "approval"},
    "ad_hocvcf_events_id": {"general": False, "name": "ad_hoc_damages"},
    "work_credit_idlabor": {"general": True, "name": "workcredit"},
}


@xray_recorder.capture("process_event")
def process_event(event, _):
    """
    Lambda to process all kinesis events from upstream and decide on how to store the events
    """
    LOGGER.info({"upstream_event": event})
    record_data = {}
    for record in event["Records"]:
        try:
            LOGGER.debug(
                {
                    "message": "record decoding steps",
                    "kinesis_data": record["kinesis"]["data"],
                    "base64_decode": base64.b64decode(record["kinesis"]["data"]),
                }
            )

            kinesis_event = json.loads(
                base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
            )

            dynamodb_event = json.loads(
                json.dumps(json_util.loads(kinesis_event)), parse_float=Decimal
            )

            key_event = "".join(dynamodb_event["dynamodb"]["Keys"].keys())

            event_type = dynamodb_event["eventName"]
            old_image = dynamodb_event["dynamodb"].get("OldImage", None)
            if event_type != "REMOVE":
                new_image = dynamodb_event["dynamodb"]["NewImage"]
                if ACTION[key_event]["general"]:
                    LOGGER.info("Storing general record for " + key_event + " event")
                    wo_key = new_image.get("work_order_key", None)
                    entity_type = ACTION[key_event]["name"]

                    if key_event == "approval_id":
                        process_approval(new_image, wo_key, key_event, old_image)
                    elif key_event == "retailrecon_id":
                        process_retail_recon(new_image, wo_key, key_event)
                    elif key_event == "work_credit_idlabor":
                        process_work_credit(new_image, wo_key, key_event)
                    elif key_event == "capture_id":
                        add_capture_data(new_image, "consignment")
                        add_capture_data_summary(new_image)
                    elif key_event == "pksk":
                        process_labor_status(new_image)
                    elif key_event == "condition_id":
                        LOGGER.info({"message": "Processing record from condition event.", "record": record})
                        process_condition(new_image, wo_key, key_event, entity_type)
                        add_condition_data_summary(new_image)
                    else:
                        process_record(
                            ACTION[key_event]["name"], new_image, wo_key, key_event
                        )

                else:
                    if ACTION[key_event].get("process"):
                        LOGGER.debug("Special process event")
                        ACTION[key_event]["process"](
                            new_image, ACTION[key_event]["name"]
                        )

                    pass
            else:
                if key_event == "retailrecon_id":
                    LOGGER.info(f"Deleting work_order records for {key_event} event")
                    delete_work_order(old_image)

        except MultipleInvalid as validation_error:
            message = {
                "validation_error": str(validation_error),
                "event": "processing order event",
                "action": "skipping record",
                "dynamodb_event": dynamodb_event,
                "kinesis_event": kinesis_event,
            }

            LOGGER.warning(message)

        except KeyError as key_error:
            message = {
                "key_error": str(key_error),
                "key_event": key_event,
                "action": "skipping record",
                "dynamodb_event": dynamodb_event,
            }

            LOGGER.warning(message)

        except UnicodeDecodeError as exc:
            message = "Invalid stream data, ignoring"
            reason = str(exc)
            exception = exc
            response = "N/A"

            LOGGER.warning(
                {
                    "event": message,
                    "reason": reason,
                    "record_data": record_data,
                    "exception": exception,
                    "response": response,
                }
            )
        except ClientError as c_err:
            handle_client_error(c_err, kinesis_event)


def process_approval(record, wo_key, key_event, old_record):
    # Handling summary approval record
    tires = record["order"]["condition"].pop("tires")
    entity_type = ACTION[key_event]["name"]
    work_order_number = record.get("work_order_number")
    vin = record.get("vin")

    if not work_order_number or not vin:
        #  get pfvehicle record for work_order_key
        pfvehicle = get_pfvehicle(work_order_key=wo_key)
        LOGGER.debug({"pfvehicle": pfvehicle})
        if pfvehicle:
            pfvehicle = json.loads(pfvehicle)
            work_order_number = pfvehicle["work_order_number"]
            vin = get_vin(pfvehicle["pfvehicle"])

    general_record_data = {
        "sblu": record["sblu"],
        "site_id": record["site_id"],
        "work_order_number": work_order_number,
        "vin": vin,
        "entity_type": entity_type,
        "updated": Decimal(time.time()),
    }
    general_record_data.update(
        {stringcase.snakecase(k): v for k, v in record["order"].items()}
    )
    general_record_data.update({"key_src": key_event})
    updated_by = general_record_data.get("updated_by", "UNKNOWN")

    # approval document
    put_work_order(wo_key, entity_type, general_record_data)

    # Identify damages to create, to update, to delete
    # Need to compare between new and old images + dynamodb sk to get right key for update/delete
    new_damages = record.get("order", {}).get("condition", {}).pop("damages", [])

    old_damages = old_record.get("order", {}).get("condition", {}).get("damages", []) if old_record else []

    damages = {get_damage_isdsa(damage): damage for damage in new_damages}
    current_damages = query(
        {
            "KeyConditionExpression": Key("pk").eq(f"workorder:{wo_key}")
            & Key("sk").begins_with("damage:")
        }
    ).get("Items", [])
    current_damages = {damage.get("sk", ""): damage for damage in current_damages}
    new_set_damages = set(
        map(lambda x: (get_damage_isdsa(x), get_damage_idsa(x)), new_damages)
    )
    old_set_damages = set(
        map(lambda x: (get_damage_isdsa(x), get_damage_idsa(x)), old_damages)
    )
    create_damages = new_set_damages.difference(old_set_damages)
    update_damages = new_set_damages.intersection(old_set_damages)
    delete_damages = old_set_damages.difference(new_set_damages)
    for tuple_damage in delete_damages:
        current_damage = current_damages.get(
            tuple_damage[0], current_damages.get(tuple_damage[1], {})
        )
        delete_sub_item_code = tuple_damage[0].split("#")[1]
        current_sub_item_code = current_damage.get("sub_item_code", "")
        if delete_sub_item_code == current_sub_item_code:
            key = {"pk": f"workorder:{wo_key}", "sk": current_damage["sk"]}
            LOGGER.debug({"deleting_damage": key})
            remove_item(TABLE, key)
            current_damage = current_damages.pop(current_damage["sk"])

            # Set the approved flag for any associated repair_labor_status
            # to false for this deleted damage
            current_damage["approved"] = False
            update_repair_status_approval_flag(wo_key, current_damage, updated_by)

    for tuple_damage in update_damages:
        current_damage = current_damages.get(
            tuple_damage[0], current_damages.get(tuple_damage[1], {})
        )
        update_sub_item_code = tuple_damage[0].split("#")[1]
        current_sub_item_code = current_damage.get("sub_item_code", "")

        if update_sub_item_code == current_sub_item_code:
            process_approval_damage(
                wo_key,
                current_damage["sk"],
                work_order_number,
                vin,
                record,
                damages[tuple_damage[0]],
                updated_by,
            )
            current_damages.pop(current_damage["sk"])

        else:
            process_approval_damage(
                wo_key,
                tuple_damage[0],
                work_order_number,
                vin,
                record,
                damages[tuple_damage[0]],
                updated_by,
            )

    if current_damages:
        LOGGER.warning(
            {"Current damages that were not updated or deleted": current_damages}
        )

    for tuple_damage in create_damages:
        process_approval_damage(
            wo_key,
            tuple_damage[0],
            work_order_number,
            vin,
            record,
            damages[tuple_damage[0]],
            updated_by,
        )

    # tires document
    LOGGER.debug({"tires": tires})
    for tire in tires:
        tire_record_data = {
            "sblu": record["sblu"],
            "site_id": record["site_id"],
            "work_order_number": work_order_number,
            "vin": vin,
            "entity_type": "tire",
            "updated": Decimal(time.time()),
            "source": "ECR",
            "is_estimate_assistant": "N",
        }
        tire_record_data.update({stringcase.snakecase(k): v for k, v in tire.items()})
        sk = "tire:%s" % (stringcase.snakecase(tire["location"].lower()))
        try:
            current_tire = get_work_order(pk=f"workorder:{wo_key}", sk=sk)
            LOGGER.debug({"Current tire": current_tire})
        except DynamoItemNotFound:
            put_work_order(wo_key, sk, tire_record_data)

    # summary document
    process_approval_summary(record)


def process_approval_damage(wo_key, sk, work_order_number, vin, record, damage, updated_by):
    damage_record_data = {
        "sblu": record["sblu"],
        "site_id": record["site_id"],
        "work_order_number": work_order_number,
        "vin": vin,
        "entity_type": "damage",
        "updated": Decimal(time.time()),
    }
    damage_record_data.update({stringcase.snakecase(k): v for k, v in damage.items()})

    LOGGER.debug({"damage_record": damage_record_data})
    labors = [
        build_labor(damage_record_data, labor_type)
        for labor_type in LABOR_TYPES
        if damage_record_data.get("".join((labor_type.lower(), "_labor_hours")), 0) != 0
        or damage_record_data.get("".join((labor_type.lower(), "_labor_cost")), 0) != 0
    ]

    if not labors:
        labors.append(build_labor(damage_record_data, "REPAIR"))

    LOGGER.info({"labors": labors})

    for labor in labors:
        labor_status_sk = create_isdt_key(labor)
        damage_record_data = update_labor_status(
            labor, wo_key, labor_status_sk, damage_record_data
        )

    get_overall_damage_status(damage_record_data)

    try:
        returned_record = put_work_order(wo_key, sk, damage_record_data)
        update_repair_status_approval_flag(wo_key, returned_record, updated_by)

    except KeyError:
        LOGGER.warning({"Item Code is missing": wo_key})


def update_repair_status_approval_flag(wo_key, damage, updated_by):
    # Update the approved flag in the damage repair_labor_status if it exists for this ISDSA
    # repair_part_status approved flag handled elsewhere
    try:
        isdsa = f"{damage.get('item_code', '')}#" \
                f"{damage.get('sub_item_code', '')}#" \
                f"{damage.get('damage_code', '')}#" \
                f"{damage.get('severity_code', '')}#" \
                f"{damage.get('action_code', '')}"
        repair_labor_status_sk = f"repair_labor_status:{isdsa}"
        LOGGER.debug(f"Updating approved flag for pk=workorder:{wo_key}, sk={repair_labor_status_sk}")
        status = {
            "approved": damage.get("approved", False),
            "updated": Decimal(Decimal(time.time()) * 1000),
            "updated_by": updated_by,
            "user_id": updated_by,
        }
        condition = Key("pk").eq(f"workorder:{wo_key}") & Key("sk").eq(repair_labor_status_sk)
        put_work_order(wo_key, repair_labor_status_sk, status, condition_obj=condition)

    except Exception as err:
        message = {
            "event": "Unknown error",
            "reason": str(err),
            "record": damage,
        }
        LOGGER.error(message)


def get_damage_idsa(damage):
    result = ""
    if damage:
        result = f"damage:{damage.get('itemCode', '')}#{damage.get('damageCode', '')}#{damage.get('severityCode', '')}#{damage.get('actionCode', '')}"
    return result


def get_damage_isdsa(damage):
    result = ""
    if damage:
        result = f"damage:{damage.get('itemCode', '')}#{damage.get('subItemCode', '')}#{damage.get('damageCode', '')}#{damage.get('severityCode', '')}#{damage.get('actionCode', '')}"
    return result


def process_work_credit(record, wo_key, key_event):
    LOGGER.debug({"WorkCredit event": record})
    vin = record.get("vin")
    work_order_number = record.get("work_order_number")

    if not work_order_number or not vin:
        # get pfvehicle record by site_id and work_order_number
        pfvehicle = get_pfvehicle(work_order_key=wo_key)
        LOGGER.debug({"pfvehicle": pfvehicle})
        if pfvehicle:
            pfvehicle = json.loads(pfvehicle)
            work_order_number = pfvehicle["work_order_number"]
            vin = get_vin(pfvehicle["pfvehicle"])

    general_record_data = {
        "sblu": record["sblu"],
        "site_id": record["site_id"],
        "updated": Decimal(time.time()),
        "vin": vin,
    }

    general_record_data.update({stringcase.snakecase(k): v for k, v in record.items()})

    if record["event_type"] == "WORKCREDIT.RETAILRECON.UPDATED":
        general_record_data.update(
            {"work_order_number": work_order_number, "entity_type": "workcredit"}
        )
        LOGGER.debug({"workcredit record": general_record_data})
        sk = "workcredit:damage#%s" % (record["labor"])
        put_work_order(wo_key, sk, general_record_data)

    elif record["event_type"] == "WORKCREDITFEE.RETAILRECON.UPDATED":
        general_record_data.update(
            {
                "work_order_number": record["consignment"]["referenceId"][
                    "workOrderNumber"
                ],
                "entity_type": "workcreditfee",
            }
        )
        LOGGER.debug({"workcreditfee record": general_record_data})
        sk = "workcredit:fee#%s" % (record["labor"])
        put_work_order(wo_key, sk, general_record_data)

    elif record["event_type"] == "PEDASHBOARD.LABOR.CONDITION.WORKCREDIT.UPDATE":
        general_record_data.update({"entity_type": "workcredit"})
        sk = "workcredit:damage#%s" % (record["labor"])
        LOGGER.debug({"workcredit record": general_record_data})
        put_work_order(wo_key, sk, general_record_data)

    elif record["event_type"] == "PEDASHBOARD.LABOR.FEE.WORKCREDIT.UPDATE":
        general_record_data.update({"entity_type": "workcreditfee"})
        sk = "workcredit:fee#%s" % (record["labor"])
        LOGGER.debug({"workcreditfee record": general_record_data})
        put_work_order(wo_key, sk, general_record_data)


def process_record(key_event_name, new_image, wo_key, key_event):
    work_order_number = new_image.get("work_order_number", None)
    vin = new_image.get("vin", None)
    if not work_order_number or not vin:
        pfvehicle = json.loads(get_pfvehicle(work_order_key=wo_key))
        if pfvehicle:
            vin = get_vin(pfvehicle["pfvehicle"])
            work_order_number = pfvehicle["work_order_number"]
    record_data = {
        "sblu": new_image["sblu"],
        "site_id": new_image["site_id"],
        "work_order_number": work_order_number,
        "vin": vin,
        "entity_type": key_event_name,
        "updated": Decimal(time.time()),
    }
    record_data.update(
        {stringcase.snakecase(k): v for k, v in new_image["order"].items()}
    )
    record_data.update({"key_src": key_event})

    put_work_order(wo_key, key_event_name, record_data)


def handle_client_error(c_err, record):
    """
    handle client error when storing to dynamodb
    """
    error_code = c_err.response["Error"]["Code"]
    message = {error_code: {"reason": str(c_err)}}

    if error_code in RETRY_EXCEPTIONS:
        message[error_code].update({"record": record})
        LOGGER.error(message)
        raise c_err

    elif error_code in IGNORE_EXCEPTIONS:
        message[error_code].update({"message": "ignored"})
        message[error_code].update({"record": record})
        LOGGER.warning(message)

    else:
        raise c_err
