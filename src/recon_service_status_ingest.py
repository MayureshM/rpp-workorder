"""
    rpp-service-status-ingest event handler
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
from validator.recon_service_status_ingest import validate_service_status_ingest_event
from datetime import datetime, timezone

ENV = Env()

DL_QUEUE = ENV("DL_QUEUE", None)
RPP_RECON_WORK_ORDER_TABLE = ENV("RPP_RECON_WORK_ORDER_TABLE")
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"
RETRY_EXCEPTIONS = (
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
)

patch_all()


@xray_recorder.capture()
def process_stream(event, _):
    """
    Processing for rpp-service-status kinesis stream events
    """
    LOGGER.info({"event": event})

    for record in event["Records"]:
        try:
            kinesis_event = json.loads(
                base64.b64decode(record["kinesis"]["data"]),
                parse_float=Decimal,
            )

            dynamodb_event = json.loads(
                json.dumps(json_util.loads(kinesis_event)), parse_float=Decimal
            )
            LOGGER.info({"service_status_ingest_dynamo_event": dynamodb_event})

            if dynamodb_event.get("eventName") in ["INSERT", "MODIFY"]:
                process_record(
                    dynamodb_event.get("dynamodb", {}).get("NewImage"),
                    dynamodb_event.get("dynamodb", {}).get(
                        "ApproximateCreationDateTime"
                    ),
                )

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

        except ClientError as client_error:
            LOGGER.exception(
                {
                    "message": "Client error processing record",
                    "exception_name": type(client_error),
                    "exception": str(client_error),
                }
            )
            raise
        except Exception as err:
            record.update({"reason": str(err)})
            message = {
                "event": "Unknown error",
                "reason": str(err),
                "record": record,
            }

            LOGGER.exception(message)
            sqs.send_message(DL_QUEUE, record)


def process_record(record, approx_creation_time):
    """
    process events from either db stream or retry queue if business conditions are met.
    """
    try:
        if record.get("event_name") in ["INSERT", "MODIFY"]:
            LOGGER.info({"processing_service_status_ingest_record": record})
            payload = validate_service_status_ingest_event(record)

            sk = payload.get("sk")
            key = {
                "pk": payload["pk"],
                "sk": sk,
            }

            payload["updated"] = Decimal(approx_creation_time)

            payload["updated_hr"] = str(
                datetime.fromtimestamp(
                    approx_creation_time / 1000,
                    timezone.utc,
                )
            )

            payload.pop("pk")
            payload.pop("sk")

            # Backwards compatibility for downstream service record
            payload["category_short_name"] = payload["shop_code"]
            payload["current_status"] = payload["shop_status_code"]

            update(
                table_name=RPP_RECON_WORK_ORDER_TABLE,
                key=key,
                update_dict=payload,
            )
        else:
            LOGGER.debug(f"Removing item: {record}")
            remove_item(
                table_name=RPP_RECON_WORK_ORDER_TABLE,
                key={
                    "pk": record.get("pk"),
                    "sk": record.get("sk"),
                },
            )

    except MultipleInvalid as validation_error:
        LOGGER.warning(
            {
                "message": "Record processing will be skipped",
                "reason": str(validation_error),
                "record": record,
            }
        )
