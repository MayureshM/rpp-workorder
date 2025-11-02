"""
    retail recon estimate events functions
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

from utils import sqs
from utils.dynamodb import update, remove_item
import boto3

from utils.common import (
    get_updated_hr,
    get_utc_now,
)

ENV = Env()

RETRY_QUEUE = ENV("RETRY_QUEUE", None)
DLQ = ENV("DLQ", None)
WORKORDER_TABLE = ENV("WORKORDER_TABLE")
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"
RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")
DYNAMO = boto3.resource("dynamodb")
patch_all()


@xray_recorder.capture()
def process_stream(event, _):
    """
    Processing for rpp-damage-no-cr-ingest stream events
    """
    LOGGER.info({"event": event})

    for record in event["Records"]:
        try:
            kinesis_event = json.loads(
                base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
            )

            dynamodb_event = json.loads(
                json.dumps(json_util.loads(kinesis_event)), parse_float=Decimal
            )

            LOGGER.debug({"decoded record": dynamodb_event})
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
            LOGGER.info(message)
            sqs.send_message(DLQ, record)


@xray_recorder.capture()
def process_queue(event, _):
    """
    Processing for rpp-damage-no-cr-ingest queue events
    """
    LOGGER.info({"event": event})

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
            sqs.send_message(DLQ, record)


def process_record(record):
    """
    process events from either db stream or retry queue if business conditions are met.
    :param record:
    :return:
    """

    try:
        if record["eventName"] in ["INSERT", "MODIFY"]:
            payload = record["dynamodb"]["NewImage"]
            LOGGER.info(f"Payload: {payload}")
            key = {
                "pk": payload.pop("pk"),
                "sk": payload.pop("sk"),
            }

            utc_now = get_utc_now()
            payload["updated"] = Decimal(utc_now.timestamp())
            payload["updated_hr"] = get_updated_hr(utc_now)

            LOGGER.debug({"Adding/updating record to rpp-recon-workorder table": payload})
            update(table_name=WORKORDER_TABLE, key=key, update_dict=payload)
        elif record["eventName"] == "REMOVE":
            payload = record["dynamodb"]["Keys"]
            key = {
                "pk": payload.pop("pk"),
                "sk": payload.pop("sk"),
            }

            LOGGER.debug({"Removing record from rpp-recon-workorder table: ": key})
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
        sqs.send_message(DLQ, record)
