"""
    rpp-notes kstream processor
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

from validation import valid_rpp_notes_item
from utils.dynamodb import update
from utils.common import (
    get_updated_hr,
    get_updated_source_hr,
    get_utc_now,
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
    Processing for rpp-notes events: decode from kinesis stream and process the record
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


@TRACER.capture_method(capture_response=False)
def process_record(record):
    """process_record."""

    try:
        # send to xray
        add_tracer_metadata_to_current_subsegment(record)

        valid_rpp_notes_item(record)

        if record["eventName"] == "INSERT":
            payload = record["dynamodb"]["NewImage"]

            annotation_data = {
                "site_id": payload.get("site_id", ""),
                "work_order_number": payload.get("work_order_number", ""),
                "notes": payload.get("work_order_key", ""),
            }
            add_tracer_annotation_to_current_subsegment(annotation_data)

            pk = payload.pop("pk")
            payload.pop("sk")

            key = {
                "pk": pk,
                "sk": pk,
            }

            updated_source_notes = record["dynamodb"]["ApproximateCreationDateTime"]
            updated_hr_source_notes = get_updated_source_hr(updated_source_notes)

            note_payload = {
                "notes": payload.get("notes", ""),
                "mod_user": payload["mod_user"],
                "updated_source_notes": updated_source_notes,
                "updated_source_hr_notes": updated_hr_source_notes,
            }

            utc_now = get_utc_now()
            note_payload["updated"] = Decimal(utc_now.timestamp())
            note_payload["updated_hr"] = get_updated_hr(utc_now)

            LOGGER.debug({"note payload to update": note_payload})

            update(table_name=WORKORDER_AM_TABLE, key=key, update_dict=note_payload)

    except MultipleInvalid as validation_error:
        message = {
            "validation_error": str(validation_error),
            "event": "processing dynamo event",
            "action": "skipping record",
            "dynamo_event": record,
        }

        LOGGER.warning(message)
        add_tracer_exception_to_current_subsegment(validation_error)

    except ClientError as client_error:
        message = {
            "message": "Failed to process response. Retrying...",
            "client_error": str(client_error),
            "event": "processing dynamo event",
            "action": "skipping record",
            "dynamo_event": record,
        }
        LOGGER.error(message)
        add_tracer_exception_to_current_subsegment(client_error)
        raise client_error

    except (TypeError, AttributeError) as error:
        message = {
            "t_error": str(error),
            "event": "processing dynamo event",
            "action": "skipping record",
            "dynamo_event": record,
        }
        LOGGER.error(message)
        add_tracer_exception_to_current_subsegment(error)


def add_tracer_metadata_to_current_subsegment(request):
    try:
        current_subsegment = TRACER.provider.current_subsegment()
        current_subsegment.put_metadata("record", request)
    except AttributeError as attr_err:
        LOGGER.exception(
            {"message": "Failed to send metadata to xray", "reason": attr_err, "request": request}
        )


def add_tracer_annotation_to_current_subsegment(request):
    try:
        current_subsegment = TRACER.provider.current_subsegment()
        current_subsegment.put_annotation("source", "Recon Workorder")
        [current_subsegment.put_annotation(key, value) for key, value in request.items()]
    except AttributeError as attr_err:
        LOGGER.exception(
            {"message": "Failed to send annotation to xray", "reason": attr_err, "request": request}
        )


def add_tracer_exception_to_current_subsegment(error):
    try:
        current_subsegment = TRACER.provider.current_subsegment()
        current_subsegment.add_error_flag()
        current_subsegment.add_fault_flag()
        current_subsegment.apply_status_code(500)
        current_subsegment.add_exception(error, [], True)
    except AttributeError as attr_err:
        LOGGER.exception(
            {"message": "Failed to send exception to xray", "reason": attr_err, "error": error}
        )
