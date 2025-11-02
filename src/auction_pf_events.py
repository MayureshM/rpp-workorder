"""
processor for listening to rpp-pfvehicle, rpp-pfvhext,rpp-pfvclog, rpp-pfvcfn, prr-pfrecon
kinesis stream and adding data to rpp-workorder, rpp-vehicle, rpp-location tables
"""
import base64
import json
import time as _time
from decimal import Decimal
from aws_xray_sdk.core import patch_all
import boto3
from botocore.exceptions import ClientError
from dynamodb_json import json_util
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid, Any
from aws_xray_sdk.core import xray_recorder  # noqa: F401
import stringcase
from dynamodb.store import put_work_order, delete_record
from validation import validate_pfvehicle
from validation import validate_pfrecon
from validation import validate_pfvcfn
from validation import validate_pfvcflog
from validation import validate_pfvehicle_offering
from validation import validate_pfvehicle_body
from rpp_lib.rpc import get_pfvehicle
from boto3.dynamodb.conditions import Key
from utils.common import get_vin, add_update_attributes, get_removed_attributes

patch_all()

IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"

ENV = Env()
DYNAMO = boto3.resource("dynamodb")
RPP_RECON_WORK_ORDER_TABLE = DYNAMO.Table(
    name=ENV("WORKORDER_AM_TABLE", validate=Any(str))
)


def process_stream(event, _):
    LOGGER.info({"event": event})

    t_loop = 0

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

            t_loop = t_loop + process_event(dynamodb_event)

        except MultipleInvalid as validation_error:
            message = {
                "validation_error": str(validation_error),
                "event": "processing vehicle event",
                "action": "skipping record",
                "kinesis_event": kinesis_event,
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
                    "record": record,
                    "exception": exception,
                    "response": response,
                }
            )
        except (ClientError, KeyError) as err:
            message = "Failed to update/delete the record"
            reason = err
            exception = err
            response = "N/A"

            LOGGER.error(
                {
                    "event": message,
                    "reason": reason,
                    "record": record,
                    "exception": exception,
                    "response": response,
                }
            )

    LOGGER.debug(
        {
            "event": "VEHICLE events",
            "Message": "Conclude processing vehicle stream",
            "loop_time": t_loop,
        }
    )


def remove_at_fields(event):
    keys_to_remove = [k for k in event.keys() if k.startswith('@')]
    for key in keys_to_remove:
        del event[key]


def process_event(record):
    LOGGER.info({"record": record})
    t_loop = _time.monotonic()

    if record["eventName"] == "REMOVE":
        LOGGER.warn({"NOT_IMPLEMENTED": "Deletion is not yet implemented"})
    elif record["tableName"] == "rpp-pfvehicle":
        pfvehicle_body = validate_pfvehicle_body(record["dynamodb"]["NewImage"])
        vin = get_vin(pfvehicle_body["pfvehicle"])
        remove_at_fields(record["dynamodb"]["NewImage"].get("pfvehicle", {}))
        remove_at_fields(record["dynamodb"].get("OldImage", {}).get("pfvehicle", {}))

        store_pfvehicle(
            record, vin
        )  # US1034118 pass the whole record into the store function
        pfvehicle = validate_pfvehicle(record["dynamodb"]["NewImage"])
        wo_key = pfvehicle["work_order_key"]
        store_summaryrecord(
            pfvehicle, record, vin
        )  # US1034118 pass the whole record into the store function
        validate_pfvehicle_offering(record["dynamodb"]["NewImage"])
        store_offeringrecord(
            record, vin
        )  # US1034118 pass the whole record into the store function

        # update VIN to all records if event is out of order
        update_records_with_vin(wo_key, "expense#", vin)
        update_records_with_vin(wo_key, "pfvcfn#", vin)
        update_records_with_vin(wo_key, "vcflog:body", vin)

    elif record["tableName"] == "rpp-pfrecon":
        pfrecon = validate_pfrecon(record["dynamodb"]["NewImage"])
        if pfrecon["change_status"] == "D":
            delete_expenserecord(pfrecon)
        else:
            store_expenserecord(pfrecon)
    elif record["tableName"] == "rpp-pfvcfn":
        pfvcfn = validate_pfvcfn(record["dynamodb"]["NewImage"])
        if pfvcfn["change_status"] == "D":
            delete_vcfnrecord(pfvcfn)
        else:
            store_vcfnrecord(pfvcfn)
    elif record["tableName"] == "rpp-pfvcflog":
        pfvcflog = validate_pfvcflog(record["dynamodb"]["NewImage"])
        store_pfvcflogrecord(pfvcflog)
    else:
        LOGGER.info({"Invalid Event"})

    t_loop = _time.monotonic() - t_loop

    return t_loop


def store_pfvehicle(record, vin):
    """
    Store vehicle record to dynamodb
    """

    pfvehicle_body_new = record["dynamodb"]["NewImage"]
    pfvehicle_body_old = record["dynamodb"].get("OldImage", {})

    LOGGER.debug({"PFVehicle body Record": pfvehicle_body_new})

    wo_key = pfvehicle_body_new["work_order_key"]

    record_data = {
        "sblu": pfvehicle_body_new["sblu"],
        "site_id": pfvehicle_body_new["site_id"],
        "work_order_number": pfvehicle_body_new["work_order_number"],
        "updated": Decimal(_time.time()),
        "entity_type": "pfvehicle",
    }

    if vin:
        record_data.update({"vin": vin})

    record_mapping = {
        "odoMeterReading": "odometer_reading",
        "Sell_Cert_Fee": "sell_cert_fee",
        "Sell_Fee": "sell_fee",
        "Sell_Fee_Other_1": "sell_fee_other_1",
        "Sell_Fee_Other_2": "sell_fee_other_2",
        "Sell_Prime_Fee": "sell_prime_fee",
        "Sell_Title_Fee": "sell_title_fee",
        "Seller_Check": "seller_check",
        "sellARDue": "sell_ar_due",
    }

    # Pass list of attributes to remove from the dynamodb record as they are not in the NewImage
    remove_attributes = get_removed_attributes(
        pfvehicle_body_new["pfvehicle"], pfvehicle_body_old.get("pfvehicle", {}), record_mapping
    )

    # Update the record with the NewImage
    for k, v in pfvehicle_body_new["pfvehicle"].items():
        k = record_mapping.get(k, stringcase.snakecase(k))
        record_data.update({k: v})

    put_work_order(
        workorder=wo_key,
        sk="pfvehicle:body",
        record=record_data,
        remove_attributes=remove_attributes,
    )


def store_summaryrecord(pfvehicle, record, vin):
    """
    Store summary record to rpp_recon_work_order dynamodb table
    """
    image_new = record["dynamodb"]["NewImage"]
    image_old = record["dynamodb"].get("OldImage", {})

    LOGGER.info({"Summary Record: pfvehicle Filtered DynamoDB event data": pfvehicle})

    wo_key = pfvehicle["work_order_key"]

    record_data = {
        "sblu": pfvehicle["sblu"],
        "site_id": pfvehicle["site_id"],
        "work_order_number": pfvehicle["work_order_number"],
        "entity_type": "summary",
    }

    if vin:
        record_data.update({"vin": vin})

    record_mapping = {
        "sellerName": "owner_name",
        "sellerDealerid": "dealer_number",
        "sellerGroupCode": "group_code",
        "primeCode": "business_unit",
    }

    # Pass list of attributes to remove from the dynamodb record as they are not in the NewImage
    remove_attributes = get_removed_attributes(
        image_new["pfvehicle"], image_old.get("pfvehicle", {}), record_mapping
    )

    # Update the record with the NewImage
    for k, v in pfvehicle["pfvehicle"].items():
        k = record_mapping.get(k, stringcase.snakecase(k))
        record_data.update({k: v})

    add_update_attributes(record_data, {"updated", "updated_pf"})
    put_work_order(
        workorder=wo_key,
        sk=f"workorder:{wo_key}",
        record=record_data,
        remove_attributes=remove_attributes,
        update_attribute="updated_pf"
    )

    LOGGER.info(
        {"Summary Record: Data Persisted to rpp_recon_work_order table": record_data}
    )


def store_offeringrecord(record, vin):
    """
    Store offering record to dynamodb
    """
    pfvehicle_new = record["dynamodb"]["NewImage"]
    pfvehicle_old = record["dynamodb"].get("OldImage", {})

    wo_key = pfvehicle_new["work_order_key"]

    record_data = {
        "updated": Decimal(_time.time()),
        "entity_type": "offering",
        "sblu": pfvehicle_new["sblu"],
        "site_id": pfvehicle_new["site_id"],
        "work_order_key": pfvehicle_new["work_order_key"],
    }

    if vin:
        record_data.update({"vin": vin})

    record_mapping = {
        "otherFee": "buyer_adj",
        "buyerDealerid": "buyer_number",
        "buyeRepId": "buyeRepId",
    }

    # Pass list of attributes to remove from the dynamodb record as they are not in the NewImage
    remove_attributes = get_removed_attributes(
        pfvehicle_new["pfvehicle"], pfvehicle_old.get("pfvehicle", {}), record_mapping
    )

    # Update the record with the NewImage
    for k, v in pfvehicle_new["pfvehicle"].items():
        k = record_mapping.get(k, stringcase.snakecase(k))
        record_data.update({k: v})

    put_work_order(
        workorder=wo_key,
        sk="offering",
        record=record_data,
        remove_attributes=remove_attributes,
    )

    LOGGER.info({"pfvehicle dynamo event data": record_data})


def store_expenserecord(pfrecon):
    """
    Store expense record to dynamodb
    """
    wo_key = pfrecon["work_order_key"]
    pfrecon_items = pfrecon["pfrecon"]

    record_data = {
        "updated": Decimal(_time.time()),
        "entity_type": "expense",
        "sblu": wo_key.split("#")[0],
        "site_id": pfrecon["site_id"],
        "work_order_number": pfrecon["work_order_number"],
    }

    pfvehicle = get_pfvehicle_record(wo_key)
    if pfvehicle:
        vin = get_vin(pfvehicle["pfvehicle"])
        if vin:
            record_data.update({"vin": vin})

    for key, value in pfrecon_items.items():
        record_data.update({stringcase.snakecase(key): value})

    sub_menu = record_data["recon_sub_menu"]
    rec_number = record_data["record_number"]
    put_work_order(wo_key, f"expense#{sub_menu}#{rec_number}", record_data)
    LOGGER.info({"pfrecon dynamo event data": record_data})


def store_vcfnrecord(pfvcfn):
    """
    Store vcfn record to dynamodb
    """
    LOGGER.debug({"VCFN Record": pfvcfn})

    wo_key = pfvcfn["sblu"] + "#" + pfvcfn["site_id"]
    sk = "pfvcfn#%s" % (pfvcfn["pfvcfn"]["categoryShortName"])

    record_data = {
        "entity_type": "pfvcfn",
        "updated": Decimal(_time.time()),
        "site_id": pfvcfn["site_id"],
        "sblu": pfvcfn["sblu"],
        "work_order_number": pfvcfn["work_order_number"],
    }

    pfvehicle = get_pfvehicle_record(wo_key)
    if pfvehicle:
        vin = get_vin(pfvehicle["pfvehicle"])
        if vin:
            record_data.update({"vin": vin})

    for k, v in pfvcfn["pfvcfn"].items():
        if k == "VCFNotes1":
            record_data.update({"vcfn_notes_1": v})
        elif k == "VCFNotes2":
            record_data.update({"vcfn_notes_2": v})
        else:
            record_data.update({stringcase.snakecase(k): v})

    put_work_order(wo_key, sk, record_data)


def store_pfvcflogrecord(pfvcflog):
    """
    Store vcflog to dynamodb
    """
    LOGGER.debug({"PFVCFLOG Record": pfvcflog})

    wo_key = pfvcflog["work_order_key"]

    record_data = {
        "updated": Decimal(_time.time()),
        "entity_type": "pfvcflog",
        "sblu": pfvcflog["sblu"],
        "site_id": pfvcflog["site_id"],
    }

    pfvehicle = get_pfvehicle_record(wo_key)
    if pfvehicle:
        vin = get_vin(pfvehicle["pfvehicle"])
        if vin:
            record_data.update({"vin": vin})
        record_data.update({"work_order_number": pfvehicle["work_order_number"]})

    for k, v in pfvcflog["pfvcflog"].items():
        if k == "vlnote":
            record_data.update({"vcfn_notes_1": v})
        elif k == "vlnote2":
            record_data.update({"vcfn_notes_2": v})
        elif k == "vlstat":
            record_data.update({"current_status": v})
        elif k == "vlflhr":
            record_data.update({"flag_hours": v})
        elif k == "vlethr":
            record_data.update({"hours": v})
        elif k == "vltxt":
            record_data.update({"panel_text": v})
        elif k == "vlperd":
            record_data.update({"period": v})
        elif k == "vlrating":
            record_data.update({"rating_code": v})
        elif k == "vlfcde":
            record_data.update({"record_code": v})
        elif k == "vlshdt":
            record_data.update({"status_date": v})
        elif k == "vlteam":
            record_data.update({"team_id": v})
        elif k == "vlstatp":
            record_data.update({"previous_state": v})
        elif k == "vlusid":
            record_data.update({"user_id": v})
        elif k == "vlpgm":
            record_data.update({"program_name": v})
        elif k == "vlfeecd":
            record_data.update({"national_fee_code": v})
        elif k == "vleflag":
            record_data.update({"error_flag": v})
        elif k == "vlcflag":
            record_data.update({"change_flag": v})
        elif k == "vlvcde":
            record_data.update({"vcf_rec_Code": v})
        else:
            record_data.update({stringcase.snakecase(k): v})

    record_data.update({"category_short_name": pfvcflog["pfvcflog"]["id"]["vlfabr"]})
    record_data.update({"sequence": pfvcflog["pfvcflog"]["id"]["vlseq"]})
    record_data.update({"status_date": pfvcflog["pfvcflog"]["id"]["vlshdt"]})
    record_data.update({"time_stamp": pfvcflog["pfvcflog"]["id"]["vltime"]})

    record_data.pop("id")

    sk = "vcflog:body#%s" % pfvcflog["pfvcflog"]["id"]["vldluni"]

    put_work_order(wo_key, sk, record_data)


def delete_vcfnrecord(pfvcfn):
    """
    Delete vcfn record from dynamodb
    """
    LOGGER.debug({"Delete VCFN Record": pfvcfn})

    wo_key = pfvcfn["sblu"] + "#" + pfvcfn["site_id"]
    sk = "pfvcfn#%s" % (pfvcfn["pfvcfn"]["categoryShortName"])

    delete_record(wo_key, sk)


def delete_expenserecord(pfrecon):
    """
    Delete pfrecon record from dynamodb
    """
    wo_key = pfrecon["work_order_key"]
    sub_menu = pfrecon["record_sub_menu"]
    rec_number = pfrecon["record_number"]
    delete_record(wo_key, f"expense#{sub_menu}#{rec_number}")


def get_pfvehicle_record(work_order_key):
    pfvehicle = json.loads(get_pfvehicle(work_order_key=work_order_key))
    return pfvehicle


def update_records_with_vin(work_order_key, sk_prefix, vin):
    """
    Update vin to all records with matching pk and sk
    """

    pk = f"workorder:{work_order_key}"

    LOGGER.debug({"pk": pk, "sk": sk_prefix, "vin": vin})

    key_condition_expression = Key("pk").eq(pk) & Key("sk").begins_with(sk_prefix)

    response = RPP_RECON_WORK_ORDER_TABLE.query(
        KeyConditionExpression=key_condition_expression
    )

    for item in response["Items"]:
        key = {"pk": pk, "sk": item["sk"]}

        attribute_names = {"#vin": "vin"}
        attribute_values = {":vin": vin}
        update_expression = "set #vin = :vin"
        condition_expression = "attribute_not_exists(#vin)"

        LOGGER.info(
            {
                "key": key,
                "update_expression": update_expression,
                "condition_expression": condition_expression,
                "attribute_names": attribute_names,
                "attribute_values": attribute_values,
            }
        )

        response = RPP_RECON_WORK_ORDER_TABLE.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ConditionExpression=condition_expression,
            ExpressionAttributeNames=attribute_names,
            ExpressionAttributeValues=attribute_values,
            ReturnValues="UPDATED_NEW",
        )

        return response["Attributes"]
