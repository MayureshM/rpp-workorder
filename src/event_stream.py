"""
processor for approval stream
"""
import base64
import copy
import json
from datetime import timezone
from decimal import Decimal
from time import monotonic

import boto3
import simplejson as s_json
# pylint: disable=unused-import
from aws_xray_sdk.core import xray_recorder  # noqa: F401
from aws_xray_sdk.core import patch_all
from botocore.exceptions import ClientError
from dateutil import parser
from dynamodb_json import json_util
from environs import Env
from rpp_lib.error_handling import format_sqs_error, invalid_queue_message
from rpp_lib.logs import LOGGER
from rpp_lib.rpc import get_unit
from rpp_lib.validation import validate_unit
from voluptuous import Any, MultipleInvalid

from labor_status import get_labor_status
from order_approval import get_order_approval
from order_certification import get_order_certification
from order_condition import get_order_condition
from order_detail import get_order_detail
from order_offering import get_order_offering
from order_retailrecon import get_order_retailrecon
from validation import valid_new_image
from vcf_events import get_vcf_events
from work_credit import get_work_credit

patch_all()

RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")

IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"

ENV = Env()
SQS = boto3.resource("sqs")
QUEUE = SQS.get_queue_by_name(QueueName=ENV("WORKORDER_QUEUE", validate=Any(str)))
DYNAMO = boto3.resource("dynamodb")
KINESIS = boto3.client("kinesis")
TABLE = DYNAMO.Table(ENV("WORKORDER_TABLE"))
MAX_STREAM_WAIT = ENV("MAX_STREAM_WAIT", 60000)

COLUMN = {
    "approval_id": {"function": get_order_approval, "unit": True, "old_image": True},
    "certification_id": {
        "function": get_order_certification,
        "unit": True,
        "old_image": False,
    },
    "condition_id": {"function": get_order_condition, "unit": True, "old_image": False},
    "detail_id": {"function": get_order_detail, "unit": True, "old_image": False},
    "offering_id": {"function": get_order_offering, "unit": True, "old_image": False},
    "retailrecon_id": {
        "function": get_order_retailrecon,
        "unit": True,
        "old_image": False,
    },
    "rpp-recon-labor-status": {
        "function": get_labor_status,
        "unit": False,
        "old_image": False,
    },
    "ad_hocvcf_events_id": {
        "function": get_vcf_events,
        "unit": True,
        "old_image": False,
    },
    "work_credit_idlabor": {
        "function": get_work_credit,
        "unit": False,
        "old_image": False,
    }
}


# pylint: disable=broad-except, invalid-name
# pylint: disable=invalid-name


def handle_client_error(c_err, record, retry_queue=True):
    """
    handle client error when storing to dynamodb
    """
    error_code = c_err.response["Error"]["Code"]
    message = {error_code: {"reason": str(c_err)}}

    if error_code in RETRY_EXCEPTIONS:
        message[error_code].update({"record": record})
        LOGGER.error(message)
        raise c_err

    elif error_code in IGNORE_EXCEPTIONS:
        message[error_code].update({"message": "ignored"})
        message[error_code].update({"record": record})
        LOGGER.warning(message)

    elif retry_queue:
        try:
            response = QUEUE.send_message(MessageBody=json.dumps(record))
        except TypeError:
            response = QUEUE.send_message(MessageBody=s_json.dumps(record))

        message[error_code].update(
            {
                "event": "added to queue, will try again later",
                "response": response,
                "record": record,
            }
        )
        LOGGER.warning(message)
    else:
        raise c_err


def lookup_unit(new_record):
    """
    call rpp_lib.rpc.get_unit to retrieve unit for this WO
    """
    unit_id = new_record["consignment"]["unit"]["href"].rsplit("/", 1)[-1]

    LOGGER.debug({"unit_id": unit_id})

    unit = get_unit(unit_id)
    if "errorMessage" in unit:
        error_response = {
            "Error": {"Code": unit["errorType"], "Message": unit["errorMessage"]}
        }
        c_err = ClientError(error_response, "remote:FIND_UNIT")
        raise c_err

    return unit


# pylint: disable=too-many-locals, too-many-arguments
def store_wo_record(new_image, updated, column, unit, table, new_column=False):
    """
    Store workorder service to dynamodb
    """
    work_order_key = new_image["sblu"]
    work_order_key += "#"
    work_order_key += new_image["site_id"]

    LOGGER.debug({"work_order_key": work_order_key})

    if isinstance(updated, float):
        updated = Decimal(str(updated))

    if isinstance(updated, str):
        updated = Decimal(updated)

    LOGGER.debug({column["name"]: column})

    key = {"work_order_key": work_order_key, "site_id": new_image["site_id"]}

    update_expression = "set \
    #sblu = :sblu,\
    #vin = :vin,\
    #work_order_number = :work_order_number,\
    #manheim_account_number = :manheim_account_number,\
    #company_name = :company_name,\
    #group_code = :group_code,\
    #check_in_date = :check_in_date,\
    #column_updated = :updated"

    condition_expression = "attribute_not_exists(#column_updated) OR \
    #column_updated < :updated"

    expression_attribute_names = {
        "#sblu": "sblu",
        "#vin": "vin",
        "#work_order_number": "work_order_number",
        "#manheim_account_number": "manheim_account_number",
        "#company_name": "company_name",
        "#group_code": "group_code",
        "#check_in_date": "check_in_date",
        "#column_updated": column.get("updated_name", column["name"] + "_updated"),
    }

    expression_attribute_values = {
        ":sblu": new_image["sblu"],
        ":vin": new_image["vin"],
        ":work_order_number": new_image["work_order_number"],
        ":manheim_account_number": new_image["consignment"]["manheimAccountNumber"],
        ":company_name": unit["contact"]["companyName"],
        ":group_code": unit["account"]["groupCode"],
        ":check_in_date": new_image["consignment"]["checkInDate"],
        ":updated": updated,
    }

    if column is not None and new_column:

        copy_data = copy.deepcopy(column["data"])
        for k, v in copy_data.items():
            if v == "Remove":
                column["data"].pop(k)

        update_expression += ", #column_name = :column_data"
        condition_expression = "(attribute_not_exists(#column_updated) OR \
            #column_updated < :updated) AND \
            attribute_not_exists(#column_name)"

        expression_attribute_names.update({"#column_name": column["name"]})

        expression_attribute_values.update({":column_data": column["data"]})

    if column is not None and not new_column:
        update_expression += "," + ",".join(
            [
                "#rpp_"
                + column["name"]
                + ".#rpp_"
                + k
                + " = :"
                + column["name"]
                + "_"
                + k
                for k in column["data"].keys()
                if column["data"][k] != "Remove"
            ]
        )

        expression_attribute_names.update({"#rpp_" + column["name"]: column["name"]})

        expression_attribute_names.update(
            {"#rpp_" + k: k for k in column["data"].keys()}
        )

        expression_attribute_values.update(
            {
                ":" + column["name"] + "_" + k: column["data"][k]
                for k in column["data"].keys()
                if column["data"][k] != "Remove"
            }
        )

    remove_fields = [
        "#rpp_" + column["name"] + ".#rpp_" + k
        for k in column["data"].keys()
        if column["data"][k] == "Remove"
    ]

    LOGGER.info({"remove_fields": remove_fields})
    LOGGER.info({"column data": column["data"]})

    if remove_fields:
        update_expression += " REMOVE " + ",".join(remove_fields)

    LOGGER.info(
        {
            "message": "store record via the following",
            "update_expression": update_expression,
            "condition_expression": condition_expression,
            "expression_attribute_names": expression_attribute_names,
            "expression_attribute_values": expression_attribute_values,
        }
    )

    response = None
    try:
        response = table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ConditionExpression=condition_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW",
        )
    except ClientError as c_err:
        error_code = c_err.response["Error"]["Code"]
        if error_code == "ValidationException":
            column.get("store_wo_record", store_wo_record)(
                new_image, updated, column, unit, table, new_column=True
            )
        else:
            raise c_err

    return response


def default_column(new_image):
    """
    default column handler function,
    logs and returns null
    """

    LOGGER.error({"message": "Unknown event type", "event_data": new_image})


def handle_new_image(new_image, updated, key, old_image):
    """
    handle dynamodb stream event
    """
    params = {"new_image": new_image, "old_image": old_image}

    if not COLUMN.get(key, default_column)["old_image"]:
        params.pop("old_image")

    column = COLUMN.get(key, default_column)["function"](**params)
    unit = None
    response = None

    if column is not None:
        if COLUMN.get(key, default_column)["unit"]:
            response = lookup_unit(new_image)
            unit = validate_unit(response)
            new_record = valid_new_image(new_image)
        else:
            new_record = new_image
        column.get("store_wo_record", store_wo_record)(
            new_record, updated, column, unit, TABLE
        )
    else:
        LOGGER.info(
            {
                "event": "unrecognized data, skipping",
                "key": key,
                "column": column,
                "record": new_image,
            }
        )


def send_to_queue(record):
    """
    just process via queue
    """

    stream_event = json.loads(json.dumps(json_util.loads(record)), parse_float=Decimal)

    LOGGER.info({"dynamo_record": stream_event})

    try:
        if stream_event["eventName"] != "REMOVE":
            response = QUEUE.send_message(
                MessageBody=json.dumps(stream_event["dynamodb"])
            )

            message = "added to queue for processing"
            record = stream_event["dynamodb"]

    except TypeError:
        response = QUEUE.send_message(
            MessageBody=s_json.dumps(stream_event["dynamodb"])
        )

        message = "added to queue for processing"
        record = stream_event["dynamodb"]

    except Exception as exc:
        message = "Invalid stream data, ignoring"
        record = stream_event
        reason = str(exc)
        response = "N/A"

        LOGGER.exception(
            {"event": message, "record": record, "reason": reason, "response": response}
        )

    LOGGER.info({"event": message, "record": record, "response": response})


def process_record(record):
    new_image = None
    try:
        LOGGER.debug({"dynamo_record": record})
        if record["eventName"] != "REMOVE":

            new_image = record["dynamodb"]["NewImage"]
            old_image = record["dynamodb"].get("OldImage", {})

            key = "".join(record["dynamodb"]["Keys"].keys())
            # need to use the table name for tables that have adjacency matrix structure
            if "pksk" in key:
                key = record["tableName"]

            try:
                if (
                    record.get("dynamodb", {})
                    .get("NewImage", {})
                    .get("retrigger_flag", False)
                ):
                    updated = record["dynamodb"]["NewImage"]["updated"]
                    LOGGER.debug({"message": "using updated timestamp"})
                else:
                    updated = (
                        parser.parse(
                            record["dynamodb"]["NewImage"]["order"]["updatedOn"]
                        )
                        .replace(tzinfo=timezone.utc)
                        .timestamp()
                    )
                    LOGGER.debug(
                        {"message": "using updatedOn datetime converted to timestamp"}
                    )
            except (KeyError, ValueError):
                LOGGER.debug({"message": "using approximate creation datetime"})
                updated = record["dynamodb"]["ApproximateCreationDateTime"]

            LOGGER.debug({"keys": key})
            LOGGER.debug({"updated_timestamp": updated})
            LOGGER.info({"handle_new_image": new_image})
            handle_new_image(new_image, updated, key, old_image)

    except ClientError as c_err:
        handle_client_error(c_err, record["dynamodb"])

    except MultipleInvalid as validation_error:
        message = {
            "validation_error": str(validation_error),
            "event": "processing stream",
            "action": "skipping record",
            "stream_event": record,
            "new_image": new_image,
        }

        LOGGER.error(message)

    except Exception as exc:
        try:
            reason = str(exc)
            response = QUEUE.send_message(MessageBody=json.dumps(record["dynamodb"]))

            message = "added to queue, will try again later"
            record = record["dynamodb"]

        except TypeError:
            response = QUEUE.send_message(MessageBody=s_json.dumps(record["dynamodb"]))

            message = "added to queue, will try again later"
            record = record["dynamodb"]

        except Exception as exc:
            message = "Invalid stream data, ignoring"
            record = record
            reason = str(exc)
            response = "N/A"

            LOGGER.exception(
                {
                    "event": message,
                    "record": record,
                    "reason": reason,
                    "response": response,
                }
            )


def process_queue(event, _):
    """
    handle queue event
    """
    LOGGER.debug({"process_queue_event": event})

    try:
        queue_event = None
        for record in event["Records"]:
            queue_event = json.loads(
                json.dumps(json_util.loads(record["body"])), parse_float=Decimal
            )
            new_image = queue_event["NewImage"]
            old_image = queue_event.get("OldImage", {})
            key = "".join(queue_event["Keys"].keys())
            # need to use the table name for tables that have adjacency matrix structure
            if "pksk" in key:
                key = record["tableName"]

            updated = queue_event["ApproximateCreationDateTime"]

            handle_new_image(new_image, updated, key, old_image)

    except MultipleInvalid as validation_error:
        message = {
            "validation_error": str(validation_error),
            "event": "processing queue",
            "action": "skipping record",
            "queu_event": queue_event,
            "new_image": new_image,
        }

        LOGGER.error(message)
        invalid_queue_message(record, validation_error)

    except ClientError as c_err:
        handle_client_error(c_err, queue_event, retry_queue=False)

    except KeyError as key_error:
        message = {
            "key_error": str(key_error),
            "record": format_sqs_error(record),
            "queue_event": queue_event,
        }
        LOGGER.error(message)
        invalid_queue_message(record, key_error)

    except IndexError as i_error:
        message = {
            "key_error": str(i_error),
            "record": format_sqs_error(record),
            "queue_event": queue_event,
        }
        LOGGER.error(message)
        invalid_queue_message(record, i_error)

    except Exception as exc:
        LOGGER.error(
            {
                "reason": str(exc),
                "exception": exc,
                "queue_event": queue_event,
                "record": format_sqs_error(record),
            }
        )
        raise exc


def process_stream(event, _):
    """
    processing kinesis stream
    """

    LOGGER.debug({"event": event})
    t_loop = monotonic()
    t_process = 0

    decoded_records = [decode_record(record) for record in event["Records"] if record]

    for record in decoded_records:
        try:
            t_process = monotonic()
            process_record(record)
            t_process += monotonic() - t_process

        except ClientError as c_err:
            handle_client_error(c_err, record, "aws:kinesis")

    t_loop = monotonic() - t_loop

    LOGGER.info(
        {"count": len(event["Records"]), "loop_time": t_loop, "process_time": t_process}
    )


def decode_record(record):
    decoded_record = None

    try:
        decoded_record = json.loads(
            json.dumps(
                json_util.loads(
                    json.loads(
                        base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
                    )
                )
            ),
            parse_float=Decimal,
        )

    except UnicodeDecodeError as exc:
        message = "Invalid stream data, ignoring"
        record = record
        reason = str(exc)
        exception = exc
        response = "N/A"

        LOGGER.warning(
            {
                "event": message,
                "record": record,
                "reason": reason,
                "exception": exception,
                "response": response,
            }
        )

    return decoded_record
