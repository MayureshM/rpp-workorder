import base64
import time
from decimal import Decimal
import json
from environs import Env
from dynamodb_json import json_util
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid
from validation import valid_rejection
from dynamodb.store import put_work_order, delete_record

ENV = Env()


def process_rejection(event, _):
    ''' Function that consumes rejection event and creates a record in
        the Adjiacency Matrix table for workorder.
    '''
    LOGGER.debug({"Event": event})

    for record in event["Records"]:
        try:
            LOGGER.debug(
                {
                    "message": "record decoding steps",
                    "kinesis_data": record["kinesis"]["data"],
                    "base64_decode": json.loads(base64.b64decode(record["kinesis"]["data"])),
                }
            )

            kinesis_event = json.loads(
                base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
            )

            dynamodb_event = json.loads(
                json.dumps(json_util.loads(kinesis_event)), parse_float=Decimal
            )

            if dynamodb_event["eventName"] == "REMOVE":
                key = dynamodb_event["dynamodb"]["Keys"]
                if ":" in key["pk"]:
                    workorder = key["pk"].split(":", 1)[1]
                    sk = key["sk"]
                    delete_record(workorder, sk)
            else:
                rejection = valid_rejection(dynamodb_event["dynamodb"]["NewImage"])

                wo_key = rejection["sblu"] + "#" + rejection["site_id"]
                rejection["updated"] = Decimal(time.time())
                rejection["entity_type"] = "rejection"

                LOGGER.debug({
                    "rejection_record": rejection,
                })

                put_work_order(wo_key, "rejection", rejection)

        except MultipleInvalid as validation_error:
            message = {
                "validation_error": str(validation_error),
                "event": "processing rejection event",
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
                    "rejection": rejection,
                    "exception": exception,
                    "response": response,
                }
            )
        except TypeError as type_error:
            message = "Invalid type, cannot store the record"
            reason = str(type_error)
            exception = type_error
            response = "N/A"

            LOGGER.warning(
                {
                    "event": message,
                    "record": rejection,
                    "reason": reason,
                    "exception": exception,
                    "response": response,

                }
            )
