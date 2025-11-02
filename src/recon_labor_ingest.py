"""
    rpp-labor-ingest event handler
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
from dynamodb.store import get_work_order
from validator.recon_labor_ingest import validate_labor_ingest_event
from datetime import datetime, timezone
import stringcase

ENV = Env()

RETRY_QUEUE = ENV("RETRY_QUEUE", None)
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
    Processing for rpp-labor-ingest kinesis stream events
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

            if dynamodb_event.get("eventName") in ["INSERT", "MODIFY"]:
                process_record(
                    dynamodb_event.get("dynamodb", {}).get("NewImage"),
                    dynamodb_event.get("dynamodb", {}).get(
                        "ApproximateCreationDateTime"
                    ),
                )
            else:
                LOGGER.debug(f"Removing item: {dynamodb_event}")
                remove_item(
                    table_name=RPP_RECON_WORK_ORDER_TABLE,
                    key={
                        "pk": dynamodb_event.get("dynamodb", {})
                        .get("Keys")
                        .get("pk"),
                        "sk": dynamodb_event.get("dynamodb", {})
                        .get("Keys")
                        .get("sk"),
                    },
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
            error_code = client_error.response["Error"]["Code"]
            if error_code in [IGNORE_EXCEPTIONS]:
                LOGGER.warning(
                    {
                        "message": "Client error processing record",
                        "exception_name": type(client_error),
                        "exception": str(client_error),
                    }
                )
            else:
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
        LOGGER.info({"processing_labor_ingest_record": record})
        payload = validate_labor_ingest_event(record)

        sk = payload.get("sk")
        key = {
            "pk": payload["pk"],
            "sk": sk,
        }

        approved, updated_by = update_approved_flag(payload)
        payload['approved'] = approved
        if updated_by:
            payload['updated_by'] = updated_by
            payload['user_id'] = updated_by

        payload["updated"] = approx_creation_time

        payload["updated_hr"] = str(
            datetime.fromtimestamp(
                approx_creation_time / 1000,
                timezone.utc,
            )
        )

        payload.pop("pk")
        payload.pop("sk")
        payload.pop("event_body", None)

        update(
            table_name=RPP_RECON_WORK_ORDER_TABLE,
            key=key,
            update_dict=payload,
        )

    except MultipleInvalid as validation_error:
        LOGGER.warning(
            {
                "message": "Record processing will be skipped",
                "reason": str(validation_error),
                "record": record,
            }
        )


def update_approved_flag(payload):
    # Check for approval
    updated_by = ''
    try:
        pk = payload["pk"]
        approval = get_work_order(pk=pk, sk="approval")

        for damage in (approval if approval else {}).get('condition', {}).get("damages", []):
            if all(damage[key] == payload[stringcase.snakecase(key)] for key in
                   ['itemCode', 'subItemCode', 'damageCode', 'severityCode', 'actionCode']):
                approved_timestamp = approval.get('updated', '')
                # If this wo was approved after the last update for the repair_labor_status
                # Copy the approval updated field
                if approved_timestamp > payload.get('updated', ''):
                    updated_by = approval.get('updated_by', '')
                return damage.get("approved", False), updated_by

    except Exception as err:
        message = {
            "event": "Unknown error: could not update approval flag",
            "reason": str(err),
            "record": payload,
        }
        LOGGER.warning(message)

    # Default False if we get here
    return False, updated_by
