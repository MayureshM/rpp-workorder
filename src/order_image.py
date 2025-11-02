"""Processor for Order Image events."""

# Standard library imports
import base64
from decimal import Decimal
import json
import time

# Third party imports
from dynamodb_json import json_util
from aws_lambda_powertools import Logger, Tracer
from voluptuous import MultipleInvalid, Any
from codeguru_profiler_agent import with_lambda_profiler
from environs import Env
from botocore.exceptions import ClientError

# Local application imports
from validation import valid_order_image
from dynamodb.store import put_work_order
from utils.common import (
    log_execution_time,
    add_tracer_metadata_to_current_subsegment,
    add_tracer_annotation_to_current_subsegment,
    add_tracer_exception_to_current_subsegment,
)
from utils.dynamodb import convert_to_date_stamp


ENV = Env()
LOGGER = Logger()
TRACER = Tracer()
PROFILE_GROUP = name = ENV("AWS_CODEGURU_PROFILER_GROUP_NAME", validate=Any(str))


@log_execution_time("KINESIS.ORDER.IMAGE.PROCESSOR")
@TRACER.capture_lambda_handler
@LOGGER.inject_lambda_context
@with_lambda_profiler(profiling_group_name=PROFILE_GROUP)
def process_order_image(event, _):
    """Function that consumes order image event and updates imaging status for the summary record."""
    LOGGER.debug({"Event": event})

    for record in event["Records"]:
        try:
            # Add only essential metadata to avoid UDP size limits
            essential_metadata = {
                "event_source": record.get("eventSource"),
                "event_id": record.get("eventID"),
                "kinesis_sequence": record.get("kinesis", {}).get("sequenceNumber"),
                "kinesis_partition": record.get("kinesis", {}).get("partitionKey")
            }
            add_tracer_metadata_to_current_subsegment(essential_metadata)

            LOGGER.debug(
                {
                    "message": "record decoding steps",
                    "kinesis_data": record["kinesis"]["data"],
                    "base64_decode": json.loads(
                        base64.b64decode(record["kinesis"]["data"])
                    ),
                }
            )

            kinesis_event = json.loads(
                base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
            )

            dynamodb_event = json.loads(
                json.dumps(json_util.loads(kinesis_event)), parse_float=Decimal
            )

            if dynamodb_event["eventName"] in ["INSERT", "MODIFY"]:
                order_image_event = valid_order_image(
                    dynamodb_event["dynamodb"]["NewImage"]
                )
                sblu = str(order_image_event["sblu"])
                site_id = order_image_event["site_id"]
                wo_key = sblu + "#" + site_id
                LOGGER.info({"wo_key": wo_key})

                annotation_data = {"sblu": sblu, "site_id": site_id, "wo_key": wo_key}

                add_tracer_annotation_to_current_subsegment(annotation_data)

                record_data = process_data(order_image_event, wo_key)
        except MultipleInvalid as validation_error:
            message = {
                "validation_error": str(validation_error),
                "event": "processing order image event",
                "action": "skipping record",
                "kinesis_event": kinesis_event,
                "dynamodb_event": dynamodb_event,
            }

            LOGGER.exception(message)
            add_tracer_exception_to_current_subsegment(validation_error)
        except UnicodeDecodeError as exc:
            message = "Invalid stream data, ignoring"
            reason = str(exc)
            exception = exc
            response = "N/A"

            LOGGER.exception(
                {
                    "event": message,
                    "reason": reason,
                    "order_image_event": order_image_event,
                    "exception": exception,
                    "response": response,
                }
            )
            add_tracer_exception_to_current_subsegment(exc)
        except TypeError as type_error:
            message = "Invalid type, cannot store the record"
            reason = str(type_error)
            exception = type_error
            response = "N/A"

            LOGGER.exception(
                {
                    "event": message,
                    "record": record_data,
                    "reason": reason,
                    "exception": exception,
                    "response": response,
                }
            )
            add_tracer_exception_to_current_subsegment(type_error)
        except KeyError as key_error:
            message = "Invalid key, cannot store the record"
            reason = str(key_error)
            exception = key_error
            response = "N/A"

            LOGGER.exception(
                {
                    "event": message,
                    "record": record_data,
                    "reason": reason,
                    "exception": exception,
                    "response": response,
                }
            )
            add_tracer_exception_to_current_subsegment(key_error)
        except ClientError as client_error:
            # Handle DynamoDB ConditionalCheckFailedException and other client errors
            error_code = client_error.response.get('Error', {}).get('Code', 'Unknown')
            message = f"Client error occurred: {error_code}"
            reason = str(client_error)

            if error_code == 'ConditionalCheckFailedException':
                message = "Conditional check failed - item may have been modified by another process"
                LOGGER.warning(
                    {
                        "event": message,
                        "error_code": error_code,
                        "reason": reason,
                        "wo_key": wo_key,
                        "action": "skipping record due to conditional check failure"
                    }
                )
            else:
                LOGGER.exception(
                    {
                        "event": message,
                        "error_code": error_code,
                        "reason": reason,
                        "record": order_image_event,
                        "exception": client_error,
                    }
                )
            add_tracer_exception_to_current_subsegment(client_error)


def process_data(order_image, wo_key):
    update_wo_record = False
    record_data = {}

    for image in order_image["order"]["images"]["images"]:
        image_category = image.get("category", "")
        if image_category == "EXT":
            update_wo_record = True
            break

    if update_wo_record:
        current_time = time.time()
        record_data = {
            "imaging_status": "COMPLETED",
            "updated": Decimal(current_time),
            "hr_updated": convert_to_date_stamp(current_time),
        }
        put_work_order(
            workorder=wo_key,
            sk=f"workorder:{wo_key}",
            record=record_data,
        )
        LOGGER.info(
            {
                "Summary Record: Data Persisted to rpp_recon_work_order table": record_data
            }
        )

    return record_data
