"""
Enhanced rpp-notes kstream processor for workorder notes with validation
"""

import json
import base64
from decimal import Decimal
from dynamodb_json import json_util
from botocore.exceptions import ClientError
from codeguru_profiler_agent import with_lambda_profiler
from voluptuous import MultipleInvalid, Any
from aws_lambda_powertools import Tracer, Logger
from environs import Env

from utils.dynamodb import update
from validator.enhanced_notes import valid_enhanced_notes_item
from utils.common import (
    add_tracer_annotation_to_current_subsegment,
    add_tracer_exception_to_current_subsegment,
    safe_json_for_logging,
)


ENV = Env()
TRACER = Tracer()
LOGGER = Logger()
PROFILE_GROUP = name = ENV("AWS_CODEGURU_PROFILER_GROUP_NAME", validate=Any(str))
WORKORDER_AM_TABLE = ENV("WORKORDER_AM_TABLE", validate=Any(str))


@LOGGER.inject_lambda_context(log_event=True)
@TRACER.capture_lambda_handler(capture_response=False)
@with_lambda_profiler(profiling_group_name=PROFILE_GROUP)
def handler(event, _):
    """
    Processing for enhanced rpp-notes events: decode from kinesis stream and process the record
    """
    LOGGER.info({"event": event})

    batch_item_failures = []

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
                    "record": safe_json_for_logging(record),
                    "response": response,
                }
            )
        except Exception as exc:
            # Add failed record to batch failures for retry
            sequence_number = record.get("kinesis", {}).get("sequenceNumber")
            if sequence_number:
                batch_item_failures.append({"itemIdentifier": sequence_number})

            LOGGER.exception(
                {
                    "message": "Failed to process enhanced notes record",
                    "error": str(exc),
                    "sequence_number": sequence_number,
                    "record": safe_json_for_logging(record),
                }
            )

    # Return batch item failures for Kinesis to retry
    return {"batchItemFailures": batch_item_failures}


@TRACER.capture_method(capture_response=False)
def process_record(record):
    """
    Process enhanced notes record with validation and DynamoDB storage
    """
    try:
        LOGGER.debug({"record": safe_json_for_logging(record)})
        # Send to xray
        add_tracer_metadata_to_current_subsegment(record)

        # Validate the record against enhanced schema
        valid_enhanced_notes_item(record)

        if record["eventName"] == "INSERT":
            payload = record["dynamodb"]["NewImage"]

            annotation_data = {
                "site_id": payload.get("site_id", ""),
                "work_order_number": payload.get("work_order_number", ""),
                "vin": payload.get("vin", ""),
                "user_id": payload.get("user_id", ""),
                "source": payload.get("source", ""),
                "type": payload.get("type", ""),
            }
            add_tracer_annotation_to_current_subsegment(
                annotation_data, source="Enhanced Workorder Notes"
            )

            # Construct DynamoDB key
            key = {
                "pk": payload.pop("pk"),
                "sk": payload.pop("sk"),
            }

            LOGGER.debug({"enhanced_note": safe_json_for_logging(payload)})

            # Store to DynamoDB using update operation
            update(table_name=WORKORDER_AM_TABLE, key=key, update_dict=payload)

            LOGGER.info(
                {
                    "message": "Enhanced note processed successfully",
                    "pk": key["pk"],
                    "sk": key["sk"],
                    "work_order_number": payload.get("work_order_number", ""),
                    "user_id": payload.get("user_id", ""),
                    "source": payload.get("source", ""),
                }
            )

    except MultipleInvalid as validation_error:
        message = {
            "validation_error": str(validation_error),
            "event": "processing enhanced notes dynamo event",
            "action": "skipping record",
            "dynamo_event": safe_json_for_logging(record),
        }

        LOGGER.warning(message)
        add_tracer_exception_to_current_subsegment(validation_error)

    except ClientError as client_error:
        message = {
            "message": "Failed to process enhanced notes response. Retrying...",
            "client_error": str(client_error),
            "event": "processing enhanced notes dynamo event",
            "action": "retrying record",
            "dynamo_event": safe_json_for_logging(record),
        }
        LOGGER.error(message)
        add_tracer_exception_to_current_subsegment(client_error)
        raise client_error

    except (TypeError, AttributeError) as error:
        message = {
            "error": str(error),
            "event": "processing enhanced notes dynamo event",
            "action": "skipping record",
            "dynamo_event": safe_json_for_logging(record),
        }
        LOGGER.exception(message)
        add_tracer_exception_to_current_subsegment(error)


def add_tracer_metadata_to_current_subsegment(request):
    """
    Add metadata to X-Ray current subsegment
    """
    try:
        current_subsegment = TRACER.provider.current_subsegment()
        current_subsegment.put_metadata("enhanced_notes_record", request)
    except AttributeError as attr_err:
        # Sanitize request data before logging to prevent log injection (addresses CodeGuru CWE-117)
        safe_request = safe_json_for_logging(request)
        LOGGER.exception(
            {
                "message": "Failed to send metadata to xray",
                "reason": str(attr_err),
                "request": safe_request,
            }
        )
