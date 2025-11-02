"""
    rpp-parts-ingest event handler
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
from validator.recon_parts_ingest import validate_parts_ingest_event
from datetime import datetime, timezone


ENV = Env()

RETRY_QUEUE = ENV("RETRY_QUEUE", None)
DL_QUEUE = ENV("DL_QUEUE", None)
RPP_RECON_WORK_ORDER_TABLE = ENV("RPP_RECON_WORK_ORDER_TABLE")
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"
RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")

patch_all()


@xray_recorder.capture()
def process_stream(event, _):
    """
    Processing for rpp-parts-ingest kinesis stream events
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

            process_record(dynamodb_event)
        except UnicodeDecodeError as exc:
            message = "Invalid stream data, ignoring"
            reason = str(exc)
            response = "N/A"

            LOGGER.exception(
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


def process_record(record):
    """
    process events from either db stream or retry queue if business conditions are met.
    """
    try:
        LOGGER.info({"processing_part_ingest_record": record})
        validate_parts_ingest_event(record)

        if record["eventName"] in ["INSERT", "MODIFY"]:
            payload = record["dynamodb"]["NewImage"]
            event_type = payload.get("event_type")
            if event_type in ["PART.STATUS", "REPAIR.PART.STATUS", "PART.ESTIMATE"]:
                sk = payload.get("sk")

                key = {
                    "pk": payload["pk"],
                    "sk": sk,
                }

                if payload["event_name"] in ["INSERT", "MODIFY"]:

                    payload["updated"] = record["dynamodb"]["ApproximateCreationDateTime"]

                    hr_updated = str(
                        datetime.fromtimestamp(
                            record["dynamodb"]["ApproximateCreationDateTime"] / 1000,
                            timezone.utc,
                        )
                    )
                    payload["hr_updated"] = hr_updated

                    payload.pop("pk")
                    payload.pop("sk")

                    update(
                        table_name=RPP_RECON_WORK_ORDER_TABLE, key=key, update_dict=payload
                    )
                elif payload["event_name"] in ["REMOVE"]:
                    remove_item(table_name=RPP_RECON_WORK_ORDER_TABLE, key=key)

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
