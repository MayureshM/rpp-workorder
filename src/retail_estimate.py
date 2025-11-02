"""
    retail estimate events functions
"""

import json
import base64
from decimal import Decimal
from dynamodb_json import json_util
from botocore.exceptions import ClientError
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid
from environs import Env
from validation import valid_retail_estimate
from utils import sqs
from utils.dynamodb import update, remove_item


ENV = Env()

RETRY_QUEUE = ENV("RETRY_QUEUE", None)
DL_QUEUE = ENV("DL_QUEUE", None)
WORKORDER_TABLE = ENV("WORKORDER_TABLE")
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
    try:
        valid_retail_estimate(record)

        if record["eventName"] in ["INSERT", "MODIFY"]:
            payload = record["dynamodb"]["NewImage"]

            key = {
                "work_order_key": payload["pk"].split("#", 1)[1],
                "site_id": payload["site_id"] + "#" + payload["sk"],
            }
            payload.pop("site_id", None)
            payload.pop("work_order_key", None)
            update(table_name=WORKORDER_TABLE, key=key, update_dict=payload)

        elif record["eventName"] == "REMOVE":
            payload = record["dynamodb"]["OldImage"]
            key = {
                "work_order_key": payload["pk"].split("#", 1)[1],
                "site_id": payload["site_id"] + "#" + payload["sk"],
            }
            remove_item(table_name=WORKORDER_TABLE, key=key)

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
