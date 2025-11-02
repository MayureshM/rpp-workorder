import base64
import time
import stringcase
from decimal import Decimal
import json
import boto3
from environs import Env
from dynamodb_json import json_util
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid, Any

from utils.common import add_update_attributes
from validation import valid_consignment
from dynamodb.store import put_work_order
from rpp_lib.validation import validate_unit
from rpp_lib.rpc import get_unit
from botocore.exceptions import ClientError

ENV = Env()
SQS = boto3.resource("sqs")
QUEUE = SQS.get_queue_by_name(
    QueueName=ENV("WORK_ORDER_CONSIGNMENT_QUEUE", validate=Any(str))
)


def process_consignment(event, _):
    """Function that consumes consignment event and creates a record in
    the Adjiacency Matrix table for workorder.
    """
    LOGGER.debug({"Event": event})
    consignment_data = {}
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
            consignment = valid_consignment(dynamodb_event["dynamodb"]["NewImage"])

            wo_key = consignment["work_order_key"]
            consignment_record = {
                "sblu": consignment["sblu"],
                "site_id": consignment["site_id"],
                "work_order_number": consignment["work_order_number"],
                "vin": consignment["vin"],
                "entity_type": "consignment",
                "updated": Decimal(time.time()),
            }

            consignment_record.update(
                {
                    stringcase.snakecase(k): v
                    for k, v in consignment["consignment"].items()
                }
            )

            LOGGER.debug(
                {"consignment": consignment, "consignment_record": consignment_record}
            )

            put_work_order(wo_key, "consignment", consignment_record)

            # create/update summary record with check in date
            summary_record = {
                "check_in_date": consignment_record.get("check_in_date"),
                "company_name": find_company(
                    consignment_record.get("unit").get("href")
                ),
                "updated": Decimal(time.time()),
            }

            add_update_attributes(summary_record, {"updated", "updated_consignment"})
            put_work_order(wo_key, f"workorder:{wo_key}", summary_record, update_attribute="updated_consignment")

        except KeyError as k_error:
            message = {
                "validation_error": str(k_error),
                "event": "processing consignment event",
                "action": "skipping record",
                "kinesis_event": kinesis_event,
                "dynamodb_event": dynamodb_event,
            }

            LOGGER.warning(message)

        except MultipleInvalid as validation_error:
            message = {
                "validation_error": str(validation_error),
                "event": "processing consignment event",
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
                    "consignment_data": consignment_data,
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
                    "record": consignment_data,
                    "reason": reason,
                    "exception": exception,
                    "response": response,
                }
            )


def find_company(href):
    """
    call rpp_lib.rpc.get_unit to retrieve unit for this WO
    """
    LOGGER.info({"href": href})
    if href:
        unit_id = href.rsplit("/", 1)[-1]

        LOGGER.debug({"unit_id": unit_id})

        unit = validate_unit(get_unit(unit_id))
        if "errorMessage" in unit:
            error_response = {
                "Error": {"Code": unit["errorType"], "Message": unit["errorMessage"]}
            }
            c_err = ClientError(error_response, "remote:FIND_UNIT")
            raise c_err
        return str(unit["contact"]["companyName"])
