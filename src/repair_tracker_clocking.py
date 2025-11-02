"""
processor for listening to rpp-work-order DynamoKinesis Stream and adding data to rpp-repair-execution service
"""
from decimal import Decimal
import boto3
import json
from dynamodb_json import json_util
import datetime
import simplejson as s_json
import base64
from aws_xray_sdk.core import patch_all, xray_recorder
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import Any, MultipleInvalid
from botocore.exceptions import ClientError
from rpp_lib.aws import get_es

from utils import sqs
from utils.common import get_updated_hr
from dynamodb.store import delete_record
from validator.repair_tracker import validate_clocking_event, validate_es_clocks

patch_all()

ENV = Env()
DYNAMO = boto3.resource("dynamodb")
RPP_RECON_WORK_ORDER_TABLE = DYNAMO.Table(
    name=ENV("WORKORDER_AM_TABLE", validate=Any(str))
)
RETRY_QUEUE = ENV("RETRY_QUEUE")
DL_QUEUE = ENV("DL_QUEUE")
ES_HOST = ENV("ES_ENDPOINT", validate=Any(str))
LAMBDA = boto3.client("lambda")
RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"
index_name = 'rpp_repair_execution_clocks'


@xray_recorder.capture()
def process_stream(event, _):
    LOGGER.debug({"process_stream_event": event})

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

            LOGGER.debug({"dynamodb_event": dynamodb_event})

            process_event(dynamodb_event["dynamodb"], kinesis_event["eventName"], kinesis_event["tableName"])

        except MultipleInvalid as validation_error:
            message = {
                "validation_error": str(validation_error),
                "event": "processing work_order event",
                "action": "skipping record",
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
                    "record": record,
                    "exception": exception,
                    "response": response,
                }
            )
        except ClientError as err:
            handle_client_error(record, err)
        except (TypeError, KeyError) as err:
            message = "Failed to update/delete the record"
            reason = err
            exception = err
            response = "N/A"

            LOGGER.error(
                {
                    "event": message,
                    "reason": reason,
                    "record": record,
                    "exception": exception,
                    "response": response,
                }
            )
        except Exception as err:
            handle_general_exception(record, err)

    LOGGER.debug(
        {
            "event": "WORK-ORDER events",
            "Message": "Conclude processing work_order stream",
        }
    )


@xray_recorder.capture()
def process_queue(event, _):
    LOGGER.debug({"process_queue_event": event})

    for record in event["Records"]:
        try:
            record = json.loads(record["body"])

            kinesis_event = json.loads(
                base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
            )

            dynamodb_event = json.loads(
                json.dumps(json_util.loads(kinesis_event)), parse_float=Decimal
            )

            LOGGER.debug({"dynamodb_event": dynamodb_event})

            process_event(dynamodb_event["dynamodb"], kinesis_event["eventName"])

        except MultipleInvalid as validation_error:
            message = {
                "validation_error": str(validation_error),
                "event": "processing problem event queue",
                "action": "skipping record",
                "kinesis_event": kinesis_event,
                "dynamodb_event": dynamodb_event,
            }

            LOGGER.warning(message)

        except UnicodeDecodeError as exc:
            message = "Invalid queue data, ignoring"
            reason = str(exc)
            exception = exc
            response = "N/A"

            LOGGER.warning(
                {
                    "event": message,
                    "reason": reason,
                    "record": record,
                    "exception": exception,
                    "response": response,
                }
            )

        except (ClientError, TypeError, KeyError) as err:
            message = "Failed to update/delete the record"
            reason = err
            exception = err
            response = "N/A"

            LOGGER.error(
                {
                    "event": message,
                    "reason": reason,
                    "record": record,
                    "exception": exception,
                    "response": response,
                }
            )

    LOGGER.debug(
        {
            "event": "WORK-ORDER events",
            "Message": "Conclude processing work_order queue",
        }
    )


@xray_recorder.capture()
def process_event(dynamodb, event_name, table_name):
    LOGGER.debug({"record": dynamodb})

    if event_name == "REMOVE" and table_name == "rpp-recon-work-order":
        # delete the document from ES index.
        # This is not an ideal scenario but it can happen when someone manually deletes data from dynamo.
        # So, adding the logic here. JIC
        validate_es_clocks(dynamodb["OldImage"])
        delete_from_es(dynamodb["OldImage"], dynamodb["OldImage"]["sk"])
    elif table_name == "rpp-repair-execution":
        # insert/delete data into dynamo and ES index
        # send data to ES index
        if event_name == "REMOVE":
            record = validate_clocking_event(dynamodb["OldImage"])
            key = {
                "pk": record["pk"],
                "sk": get_clocking_sk(record)
            }
            delete_record(key["pk"].split(":")[1], key["sk"])
            validate_es_clocks(dynamodb["OldImage"])
            delete_from_es(dynamodb["OldImage"], key["sk"])
        elif (
                event_name == "INSERT"
                or event_name == "MODIFY"
        ):
            record = validate_clocking_event(dynamodb["NewImage"])

            pk = record["pk"]
            sk = get_clocking_sk(record)
            record["updated"] = int(Decimal(dynamodb["ApproximateCreationDateTime"] / 1000))
            key = {
                "pk": pk,
                "sk": sk
            }
            record.pop("pk", None)
            record.pop("sk", None)

            # update dynamo db table
            update_record(key, record=record)

            validate_es_clocks(dynamodb["NewImage"])
            insert_into_es(dynamodb["NewImage"])


def insert_into_es(record):
    # authorize and get an instance of ES
    _es = get_es(es_host=ES_HOST)

    pk = record["pk"]
    sk = get_clocking_sk(record)
    record.pop("sk")
    record["sk"] = sk

    _id = f"{pk}:{sk}"

    LOGGER.debug({
        "Clocking data insertion logs: ": record
    })

    es_result = _es.index(
        index=index_name,
        body=record,
        id=_id,
        doc_type="_doc"
    )

    LOGGER.debug({
        "Clocking data insertion logs: ": es_result
    })


def delete_from_es(record, sk):
    # authorize and get an instance of ES
    _es = get_es(es_host=ES_HOST)

    pk = record["pk"]

    _id = f"{pk}:{sk}"

    LOGGER.debug({
        "ES update data: ": {
            "pk": pk,
            "sk": sk
        }
    })

    es_result = _es.delete(
        index=index_name,
        id=_id,
        doc_type="_doc"
    )

    LOGGER.debug({
        "Clocking data deletion logs: ": es_result
    })


def get_clocking_sk(record):
    if record.get("pk", "").startswith("login"):
        return record["sk"]
    item_code = record.get("item_code", "")
    sub_item_code = record.get("sub_item_code", "")
    damage_code = record.get("damage_code", "")
    severity_code = record.get("severity_code", "")
    action_code = record.get("action_code", "")
    clock_time = record["sk"][6:]
    sk = "clock_rt:" + item_code + "#" + sub_item_code \
         + "#" + damage_code + "#" + severity_code + \
         "#" + action_code + ":" + clock_time

    return sk


@xray_recorder.capture()
def handle_client_error(record, err):
    reason = err
    exception = err
    response = "N/A"
    error_code = err.response["Error"]["Code"]
    message = {error_code: {"reason": str(err)}}

    if error_code in RETRY_EXCEPTIONS:
        message[error_code].update({"record": record})
        LOGGER.error(message)
        raise err
    elif error_code in IGNORE_EXCEPTIONS:
        message[error_code].update({"event": "ignored"})
        LOGGER.warning(message)
    else:
        record.update({"reason": err})
        sqs.send_message(
            RETRY_QUEUE, s_json.loads(s_json.dumps(record, default=date_to_string))
        )

        LOGGER.warning(
            {
                "event": message,
                "reason": reason,
                "record": record,
                "exception": exception,
                "response": response,
            }
        )


@xray_recorder.capture()
def handle_general_exception(record, err):
    message = "Unknown error"
    reason = err
    exception = err
    response = "N/A"

    LOGGER.exception(
        {
            "event": message,
            "reason": reason,
            "record": record,
            "exception": exception,
            "response": response,
        }
    )

    record.update({"reason": err})
    sqs.send_message(
        DL_QUEUE, s_json.loads(s_json.dumps(record, default=date_to_string))
    )


@xray_recorder.capture("update_record")
def update_record(key, record, condition=None):
    record["hr_updated"] = get_updated_hr(datetime.datetime.fromtimestamp(record["updated"], datetime.timezone.utc))

    attribute_names = {}
    attribute_values = {}

    update_expression = "set "
    update_expression += ",".join(
        map(lambda k: f"#{k} = :{k}", filter(lambda k: record[k], record.keys())))

    attribute_names.update({"#" + k: k for k in record.keys() if record[k]})

    attribute_values.update({":" + k: record[k] for k in record.keys() if record[k]})
    condition_expression = "attribute_not_exists(#updated) OR #updated <= :updated"

    if condition:
        condition_expression += " " + condition

    LOGGER.debug(
        {
            "key": key,
            "update_expression": update_expression,
            "condition_expression": condition_expression,
            "expression_attribute_names": attribute_names,
            "expression_attribute_values": attribute_values,
        }
    )

    response = RPP_RECON_WORK_ORDER_TABLE.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ConditionExpression=condition_expression,
        ExpressionAttributeNames=attribute_names,
        ExpressionAttributeValues=attribute_values,
        ReturnValues="UPDATED_NEW",
    )

    LOGGER.debug({"response": response})

    return response["Attributes"]


def date_to_string(field):
    """cast datetime to string"""
    if isinstance(field, datetime.datetime):
        return field.__str__()
