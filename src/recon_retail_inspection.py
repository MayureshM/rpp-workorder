"""
    retail recon inspection events functions
"""

import base64
import json
import time
import boto3
from decimal import Decimal

from aws_xray_sdk.core import patch_all
from aws_xray_sdk.core import xray_recorder
from botocore.exceptions import ClientError
from dynamodb_json import json_util
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid

from utils.common import add_update_attributes
from utils import sqs
from utils.constants import MEASUREMENT_LOCATION, MEASUREMENT_TYPE
from utils.dynamodb import update, delete_field_item, remove_item
from validation import valid_retail_inspection
from dynamodb.store import put_work_order


ENV = Env()

DYNAMO = boto3.resource("dynamodb")
RECON_WORK_ORDER_TABLE = ENV("RECON_WORK_ORDER_TABLE")
RECON_WORK_ORDER_TABLE_INSTANCE = DYNAMO.Table(name=RECON_WORK_ORDER_TABLE)
RETRY_QUEUE = ENV("RETRY_QUEUE", None)
DL_QUEUE = ENV("DL_QUEUE", None)
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"
RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")

patch_all()


@xray_recorder.capture()
def process_stream(event, _):
    """
    Processing for rpp-re-ingest stream events
    """
    LOGGER.debug({"event": event})

    for record in event["Records"]:
        try:
            kinesis_event = json.loads(
                base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
            )

            dynamodb_event = json.loads(
                json.dumps(json_util.loads(kinesis_event)), parse_float=Decimal
            )

            process_record(dynamodb_event)
        except UnicodeDecodeError as exc:
            message = "Invalid stream data, ignoring"
            reason = str(exc)
            response = "N/A"

            LOGGER.warning(
                {
                    "event": message,
                    "reason": reason,
                    "record": record,
                    "response": response,
                }
            )

        except ClientError as err:
            handle_client_error(err, record)
        except Exception as err:
            record.update({"reason": str(err)})
            message = {
                "event": "Unknown error",
                "reason": str(err),
                "record": record,
            }

            LOGGER.exception(message)
            sqs.send_message(DL_QUEUE, record)


@xray_recorder.capture()
def process_queue(event, _):
    """
    Processing for rpp-re-ingest queue events
    """
    LOGGER.debug({"event": event})

    for record in event["Records"]:
        try:
            record = json.loads(record["body"])
            kinesis_event = json.loads(
                base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
            )

            dynamodb_event = json.loads(
                json.dumps(json_util.loads(kinesis_event)), parse_float=Decimal
            )

            process_record(dynamodb_event)
        except UnicodeDecodeError as exc:
            message = "Invalid stream data, ignoring"
            reason = str(exc)
            response = "N/A"

            LOGGER.warning(
                {
                    "event": message,
                    "reason": reason,
                    "record": record,
                    "response": response,
                }
            )
        except ClientError as err:
            handle_client_error(err, record, retry_queue=False)
        except Exception as err:
            message = {
                "event": "Unknown error",
                "reason": str(err),
                "record": record,
            }
            LOGGER.exception(message)
            record.update({"reason": str(err)})
            sqs.send_message(DL_QUEUE, record)


def process_record(record):
    """
    process events from either db stream or retry queue if business conditions are met.
    :param record:
    :return:
    """
    LOGGER.debug({"record": record})
    try:
        valid_retail_inspection(record)

        if record["eventName"] in ["INSERT", "MODIFY"]:
            payload = record["dynamodb"]["NewImage"]
            if payload.get("sk").startswith("inspection"):

                if payload.get("item_name") == "External Reference Number":
                    # create/update summary record with client_reference_number
                    wo_key = f"{payload['sblu']}#{payload['site_id']}"
                    summary_record = {
                        "client_reference_number": payload.get("measurement"),
                        "updated": Decimal(time.time()),
                    }

                    add_update_attributes(summary_record, {"updated", "updated_ri"})
                    put_work_order(wo_key, f"workorder:{wo_key}", summary_record, update_attribute="updated_ri")

                key = {
                    "pk": payload["pk"].replace("#", ":", 1),
                    "sk": payload["sk"].replace("#", ":"),
                }
                payload.pop("pk")
                payload.pop("sk")
                payload["updated"] = Decimal(time.time())
                update_measurement_record(payload, key["pk"])
                update(table_name=RECON_WORK_ORDER_TABLE, key=key, update_dict=payload)
            elif payload.get("sk").startswith("observation"):
                key = {
                    "pk": payload["pk"].replace("#", ":", 1),
                    "sk": payload["sk"].replace("inspection#", "inspection:"),
                }

                item_code = payload.get("item_code", "")
                sub_item_code = payload.get("sub_item_code", "")
                damage_code = payload.get("damage_code", "")
                severity_code = payload.get("severity_code", "")
                action_code = payload.get("action_code", "")
                isdsa = (
                    item_code
                    + "#"
                    + sub_item_code
                    + "#"
                    + damage_code
                    + "#"
                    + severity_code
                    + "#"
                    + action_code
                )
                damage_sk = f"damage:{isdsa}"

                damage_record = RECON_WORK_ORDER_TABLE_INSTANCE.get_item(
                    Key={"pk": key["pk"], "sk": damage_sk}
                )
                damage_record = damage_record.get("Item", {})
                if damage_record:
                    payload["shop_code"] = damage_record.get("shop_code", "")
                    payload["vin"] = damage_record.get("vin", "")

                payload.pop("pk")
                payload.pop("sk")
                inspection_id = payload.pop("inspection_id", None)
                if inspection_id:
                    payload["inspection_id"] = inspection_id.replace(
                        "inspection#", "inspection:"
                    )
                payload["updated"] = Decimal(time.time())
                update(table_name=RECON_WORK_ORDER_TABLE, key=key, update_dict=payload)

            elif payload.get("sk").startswith("station"):
                key = {
                    "pk": payload["pk"].replace("#", ":", 1),
                    "sk": payload["sk"].replace("station#", "station:"),
                }

                payload.pop("pk")
                payload.pop("sk")

                payload["updated"] = Decimal(time.time())
                update(table_name=RECON_WORK_ORDER_TABLE, key=key, update_dict=payload)
            else:
                LOGGER.debug("Record processing will be skipped")

        elif record["eventName"] == "REMOVE":

            payload = record["dynamodb"]["OldImage"]
            if payload.get("sk").startswith("inspection") or payload.get(
                "sk"
            ).startswith("observation"):
                pk = "workorder:" + payload["sblu"] + "#" + payload["site_id"]
                sk = payload["sk"].replace("inspection#", "inspection:")

                key = {
                    "pk": pk,
                    "sk": sk,
                }
            elif payload.get("sk").startswith("station"):
                key = {
                    "pk": payload["pk"].replace("#", ":", 1),
                    "sk": payload["sk"].replace("station#", "station:"),
                }

            remove_item(table_name=RECON_WORK_ORDER_TABLE, key=key)

    except MultipleInvalid as validation_error:
        LOGGER.warning(
            {
                "message": "Record processing will be skipped",
                "reason": str(validation_error),
                "record": record,
            }
        )


def handle_client_error(err, record, retry_queue=True):
    error_code = err.response["Error"]["Code"]
    message = {error_code: {"reason": str(err)}}
    reason = err

    LOGGER.error({"event": message, "reason": reason, "record": record})

    if error_code in RETRY_EXCEPTIONS:
        message[error_code].update({"record": record, "event": "Retry Error"})
        LOGGER.warning(message)
        record.update({"reason": str(err)})
        if retry_queue:
            sqs.send_message(RETRY_QUEUE, record)
        else:
            raise err

    elif error_code in IGNORE_EXCEPTIONS:
        message[error_code].update(
            {
                "event": "Ignore error",
                "record": record,
            }
        )
        LOGGER.warning(message)
    else:
        record.update({"reason": str(err)})
        message[error_code].update(
            {
                "event": "Unknown error",
                "record": record,
            }
        )
        LOGGER.exception(message)
        sqs.send_message(DL_QUEUE, record)


def update_measurement_record(payload, pk):
    item_name = payload.get("item_name").split("-")
    entity_type = MEASUREMENT_TYPE.get(item_name[0].strip())
    if entity_type:
        if entity_type == "tire":
            item_name = item_name[1].strip().split(" ")
            location = ":" + MEASUREMENT_LOCATION.get(item_name[0].strip(), "")
        else:
            location = (
                ":" + MEASUREMENT_LOCATION.get(item_name[1].strip(), "")
                if len(item_name) == 2
                else ""
            )
        key = {"pk": pk, "sk": f"{entity_type}{location}"}
        if payload.get("measurement"):
            measurement_record = {
                "site_id": payload.get("site_id"),
                "vin": payload.get("vin"),
                "work_order_number": payload.get("work_order_number"),
                "entity_type": entity_type,
                "updated": Decimal(time.time()),
            }
            if entity_type == "tire":
                measurement_record["depth"] = payload.get("measurement")
                measurement_record["depth_uom"] = payload.get("uom")
                measurement_record["source"] = "Inspection Platform"
                measurement_record["is_estimate_assistant"] = "N"
            else:
                measurement_record["measurement"] = payload.get("measurement")
                measurement_record["uom"] = payload.get("uom")
            response = update(
                table_name=RECON_WORK_ORDER_TABLE,
                key=key,
                update_dict=measurement_record,
            )
            LOGGER.debug({"save measurement successful": response})
        else:
            if entity_type == "tire":
                response = delete_field_item(
                    table_name=RECON_WORK_ORDER_TABLE, key=key, attribute="depth"
                )
            else:
                response = delete_field_item(
                    table_name=RECON_WORK_ORDER_TABLE, key=key, attribute="measurement"
                )
            LOGGER.debug({"remove measurement successful": response})
