"""
processor for listening to rpp-amazon-ingest kinesis stream and adding data to rpp-recon-vehicle
"""
import base64
import json
import time as _time
from decimal import Decimal

import boto3
from aws_xray_sdk.core import patch_all
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from dynamodb_json import json_util
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import Any, MultipleInvalid
from utils.dynamodb import convert_to_date_stamp

from dynamodb.store import delete_record, put_work_order, query
from utils import sqs
from validator.amazon_ingest import (InvalidDspRecordException,
                                     validate_amazon_dsp_ingest,
                                     validate_amazon_ingest)

patch_all()

ENV = Env()
DYNAMODB = boto3.resource("dynamodb")
RECON_WORKORDER_TABLE = DYNAMODB.Table(
    name=ENV("WORKORDER_AM_TABLE", validate=Any(str))
)
RETRY_QUEUE = ENV("RETRY_QUEUE", None)
DL_QUEUE = ENV("DL_QUEUE", None)
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"
RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")


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
            sk = dynamodb_event["dynamodb"]["NewImage"]["sk"]
            if sk.startswith("dsp"):
                t_loop = t_loop + process_dsp_event(dynamodb_event)
            else:
                t_loop = t_loop + process_event(dynamodb_event)

        except MultipleInvalid as validation_error:
            message = {
                "validation_error": str(validation_error),
                "event": "processing amazon ingest event",
                "action": "skipping record",
                "kinesis_event": kinesis_event,
                "dynamodb_event": dynamodb_event,
            }

            LOGGER.warning(message)
            sqs.send_message(DL_QUEUE, record)

        except UnicodeDecodeError as exc:
            message = "Invalid stream data, ignoring"
            reason = str(exc)
            exception = exc
            response = "N/A"

            LOGGER.warning(
                {
                    "event": message,
                    "reason": reason,
                    "record": kinesis_event,
                    "exception": exception,
                    "response": response,
                }
            )
        except ClientError as err:
            handle_client_error(err, record)
        except KeyError as err:
            message = "Failed to update/delete the record"
            reason = str(err)
            exception = err
            response = "N/A"
            LOGGER.error(
                {
                    "event": message,
                    "reason": reason,
                    "record": kinesis_event,
                    "exception": exception,
                    "response": response,
                }
            )
        except Exception as err:
            message = "Unknown error"
            reason = str(err)
            exception = err
            response = "N/A"
            LOGGER.error(
                {
                    "event": message,
                    "reason": reason,
                    "record": kinesis_event,
                    "exception": exception,
                    "response": response,
                }
            )
            sqs.send_message(DL_QUEUE, record)

    LOGGER.debug(
        {
            "event": "RPP RECON VEHICLE events",
            "Message": "Conclude processing vehicle stream",
            "loop_time": t_loop
        }
    )


def process_dsp_event(record):
    LOGGER.debug({"record": record})
    t_loop = _time.monotonic()

    if record["eventName"] == "MODIFY":
        try:
            amazon_ingest = validate_amazon_dsp_ingest(
                record["dynamodb"]["NewImage"]
            )
            # Avoid duplicate dsp records
            sk_prefix = amazon_ingest["sk"].split(":")[0]
            dsp_records = query(
                {
                    "KeyConditionExpression": Key("pk").eq(f"workorder:{amazon_ingest['sblu']}#{amazon_ingest['site_id']}")
                    & Key("sk").begins_with(sk_prefix)
                }
            ).get("Items", [])
            dsp_match = False
            new_amazon_ingest = dict.copy(amazon_ingest)
            new_amazon_ingest.pop("sk")
            new_amazon_ingest.pop("pk")

            # Comparing existend with incoming records
            for record in dsp_records:
                record.pop("updated")
                record.pop("sk")
                record.pop("pk")
                differences = set(new_amazon_ingest.items()) - set(record.items())
                if not differences:
                    dsp_match = True

            if not dsp_match:
                store_amazon_ingest_record(amazon_ingest)
            else:
                message = "Order duplicated, ignoring"
                reason = "Order duplicated"
                response = "N/A"
                LOGGER.warning(
                    {
                        "event": message,
                        "reason": reason,
                        "record": amazon_ingest,
                        "response": response,
                    }
                )
        except MultipleInvalid as validation_error:
            message = {
                "validation_error": str(validation_error),
                "event": "validating dsp - agregate record",
                "action": "skipping record",
                "record": record
            }
            LOGGER.warning(message)
        except InvalidDspRecordException as dsp_exception:
            message = {
                "validation_error": str(dsp_exception),
                "event": "validating dsp constraints - agregate record",
                "action": "skipping record",
                "record": record
            }
            LOGGER.warning(message)
    t_loop = _time.monotonic() - t_loop

    return t_loop


def process_event(record):
    LOGGER.debug({"record": record})
    t_loop = _time.monotonic()

    if record["eventName"] in ["INSERT", "MODIFY"]:
        try:
            amazon_ingest = validate_amazon_ingest(record["dynamodb"]["NewImage"])
            order_cancel_date = amazon_ingest.get("order_cancel_date", None)
            status = amazon_ingest.get("status", "")
            if order_cancel_date and status.lower() in "order cancelled":
                message = "Order cancelled, ignoring"
                reason = "Order cancelled"
                response = "N/A"
                LOGGER.warning(
                    {
                        "event": message,
                        "reason": reason,
                        "record": amazon_ingest,
                        "response": response,
                    }
                )
                transport_records = query(
                    {
                        "KeyConditionExpression": Key("pk").eq(f"workorder:{amazon_ingest['sblu']}#{amazon_ingest['site_id']}")
                        & Key("sk").begins_with(f"transport:{'inbound' if amazon_ingest.get('is_inbound') else 'outbound' if amazon_ingest.get('is_outbound') else ''}")
                    }
                ).get("Items", [])
                for transport in transport_records:
                    if (amazon_ingest["vin"] in transport["vin"]
                            and amazon_ingest["work_order_number"] in transport["work_order_number"]
                            and amazon_ingest["shipper_id"] in transport["shipper_id"]):
                        LOGGER.debug({"delete record: ": transport})
                        delete_record(f"{amazon_ingest['sblu']}#{amazon_ingest['site_id']}", transport["sk"])
                return _time.monotonic() - t_loop
            transport_records = query(
                {
                    "KeyConditionExpression": Key("pk").eq(f"workorder:{amazon_ingest['sblu']}#{amazon_ingest['site_id']}")
                    & Key("sk").begins_with("transport")
                }
            ).get("Items", [])
            transport_match = False
            if "transport_lp" in amazon_ingest["sk"]:
                transport_key = amazon_ingest["vin"] + amazon_ingest["manheim_account_number"] + amazon_ingest["site_id"]
            else:
                state = "inbound" if amazon_ingest.get("is_inbound") else "outbound" if amazon_ingest.get("is_outbound") else ""
                group_load_id = amazon_ingest.get("group_load_id", "")
                transport_key = state + amazon_ingest["vin"] + amazon_ingest["invoice_number"] + group_load_id + amazon_ingest["shipper_id"]
            LOGGER.debug({"transport_key:": transport_key})
            for transport in transport_records:
                if "transport_lp" in transport["sk"]:
                    old_transport_key = transport["vin"] + transport["manheim_account_number"] + transport["site_id"]
                else:
                    state = "inbound" if transport.get("is_inbound") else "outbound" if transport.get("is_outbound") else ""
                    group_load_id = transport.get("group_load_id", "")
                    old_transport_key = state + transport["vin"] + transport["invoice_number"] + group_load_id + transport["shipper_id"]
                LOGGER.debug({"old_transport_key:": old_transport_key, "transport_key:": transport_key})
                if old_transport_key == transport_key:
                    amazon_ingest["pk"] = transport["pk"]
                    amazon_ingest["sk"] = transport["sk"]
                    transport_match = True
                    store_amazon_ingest_record(amazon_ingest)
                    break
            if not transport_match:
                store_amazon_ingest_record(amazon_ingest)
        except MultipleInvalid as validation_error:
            message = {
                "validation_error": str(validation_error),
                "event": "validating amazon ingest record",
                "action": "skipping record",
                "record": record
            }
            LOGGER.warning(message)
    t_loop = _time.monotonic() - t_loop

    return t_loop


def process_queue(event, _):
    """
    Processing for rpp-amazon-ingest queue events
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

            process_event(dynamodb_event)
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


def store_amazon_ingest_record(record):
    '''
        Store amazon amazon ingest record into rpp-recon-work-order dynamodb table
    '''
    LOGGER.debug({"Amazon dsp Record": record})

    try:
        pk = record["sblu"] + "#" + record["site_id"]
        sk = record.pop("sk")
        record.pop("pk")
        record["entity_type"] = sk.split(":")[0]
        record["updated"] = Decimal(_time.time())
        if record["entity_type"] == "dsp":
            record["vin"] = record["redeployment_receiving_vin"]
        elif record["entity_type"] == "transport":
            record["hr_updated"] = convert_to_date_stamp(record["updated"])
        put_work_order(pk, sk, record)
        LOGGER.debug({
            "Amazon Ingest Record: Data Persisted record": record
        })

    except ClientError as c_err:
        handle_client_error(c_err, record)


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
