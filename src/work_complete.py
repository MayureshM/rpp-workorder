import boto3
import json
import time
from ast import literal_eval
from decimal import Decimal

from aws_lambda_powertools.utilities.data_classes import (
    DynamoDBStreamEvent,
    SQSEvent,
)
from boto3.dynamodb.conditions import Key
from aws_xray_sdk.core import patch_all, xray_recorder
from botocore.exceptions import ClientError
from codeguru_profiler_agent import with_lambda_profiler
from environs import Env
from rpp_lib.logs import LOGGER
from typing import Union, Dict
from voluptuous import Any, Invalid, MultipleInvalid
from dynamodb_json import json_util as j_util

from dynamodb.store import update_document_for_pk_and_sk
from utils.sqs import send_message
from utils.common import sanitize_for_logging

patch_all()

ENV = Env()

DYNAMO = boto3.resource("dynamodb")
SQS = boto3.client("sqs")
DL_QUEUE = SQS.get_queue_url(QueueName=ENV("DL_QUEUE", validate=Any(str)))["QueueUrl"]
WCI_RETRY_QUEUE = SQS.get_queue_url(
    QueueName=ENV("WCI_RETRY_QUEUE", validate=Any(str))
)["QueueUrl"]
PROFILE_GROUP = name = ENV("AWS_CODEGURU_PROFILER_GROUP_NAME", validate=Any(str))
RECON_WORK_ORDER_TABLE = DYNAMO.Table(ENV("WORKORDER_AM_TABLE", validate=Any(str)))
RETRY_DELAY_SEC = literal_eval(ENV("RETRY_DELAY_SEC", validate=Any(str)))


# This mapping used to set the right complete flag into workorder summary record
COMPLETED_RECORD_SK_PREFIX_MAPPING = {
    "recon:complete": "recon_complete",
    "storage:complete": "storage_complete",
    "work:complete": "work_order_complete",
    "work:rejected": "work_rejected",
}


def get_upsert_complete_flag_obj(record_sk: str, complete_date: str) -> dict:
    """
    This function returns the respective complete flag object
    """
    sk_prefix = record_sk.split("#")[0]
    # Based on sk prefix get the complete flag attribute
    complete_flag_attr = COMPLETED_RECORD_SK_PREFIX_MAPPING.get(sk_prefix)

    if not complete_flag_attr:
        # if sk prefix mapping is not available, throw MultipleInvalid Exception
        raise Invalid(f"SK prefix mapping is not configured for '{sk_prefix}'")

    return {
        complete_flag_attr: True,
        f"{complete_flag_attr}_date": complete_date,
        "updated": Decimal(time.time())
    }


def query_table_for_given_criteria(table, criteria) -> bool:
    """
    Query the table(rpp-recon-work-order) for a given dependency criteria
    If record exists and VCF=1 (RPP case) returns true else false
    @param: table_name : the table to query
    @param: criteria : dependency criteria list
    """
    pk = criteria.get("pk")
    sk_prefix = criteria.get("sk")
    try:
        filter_exp = Key("pk").eq(pk) & Key("sk").begins_with(sk_prefix)
        response = table.query(KeyConditionExpression=filter_exp)
        if "vehicle_complete_flag" in criteria:
            if bool(response.get("Items", [])):
                record = response["Items"][0]
                return bool(record.get("vehicle_complete_flag", 0))
            return False
        else:
            return bool(response.get("Items", []))
    except Exception as e:
        LOGGER.error(
            f"Error querring table {table} for pk:{pk} and sk:{sk_prefix}. Error: {str(e)}"
        )
        return False


def update_summary_record(pk: str, sk: str, upsert_flag_obj: Dict) -> None:
    condition_expression = ""
    if "work_order_complete" in upsert_flag_obj:
        condition_expression = "attribute_not_exists(work_order_complete_date)"
    try:
        update_document_for_pk_and_sk(pk, sk, upsert_flag_obj,
                                      condition_expression=condition_expression)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ConditionalCheckFailedException':
            data_to_log = sanitize_for_logging({
                "message": "Conditional check failed",
                "error": str(e),
                "upsert_flag_obj": upsert_flag_obj
            })
            LOGGER.warning(
                data_to_log
            )
        else:
            data_to_log = sanitize_for_logging({
                "message": "DynamoDB update failed",
                "error": str(e),
                "upsert_flag_obj": upsert_flag_obj
            })
            LOGGER.exception(
                data_to_log
            )
            raise


def process_work_complete_record(
    record: dict,
    record_sk: str,
    is_dynamodb_event: bool,
    is_sqs_event: bool,
    messages_to_reprocess: list = [],
    message_id: str = "",
):
    """
    This function process the work complete ingest record
    1. If record meets criteria, upsert required fields complete flag and storage date if available into workorder summary record
    2. If any record doesn't met criteria
        a. If event is dynamoDB event, send the record to retry SQS with delay
        b. If event is SQS event, return the itemIdentifier to retry
    """
    LOGGER.info("Processing work complete ingest record")
    pk = sk = record.get("pk", "")[:]

    # Validate SK prefix and retrieve the complete flag obj
    complete_date = record.get("complete_date")
    upsert_flag_obj = get_upsert_complete_flag_obj(record_sk, complete_date)

    # check for storage date
    if record.get("storage_date"):
        upsert_flag_obj.update({"storage_date": record.get("storage_date")})
    # Add manheim_account_number to the workorder summary
    upsert_flag_obj.update(
        {"manheim_account_number": record.get("manheim_account_number")}
    )

    # The following block for dependency criteria is implemented as a part of addressing
    # the event ordering/charge-record ordering issue in AMP
    dependency_criteria = record.get("dependency_criteria")
    if dependency_criteria:
        all_criteria_met = all(
            query_table_for_given_criteria(RECON_WORK_ORDER_TABLE, criteria)
            for criteria in dependency_criteria
        )
        if all_criteria_met:
            # add the work order complete stage flag to workorder summary record
            LOGGER.info(
                f"All dependency criteria(s) are met, "
                f"setting work_complete to true in summary record for {pk}"
            )
            update_summary_record(pk, sk, upsert_flag_obj)

        elif is_dynamodb_event:
            # Send to SQS retry Queue.
            LOGGER.warning(
                f"All/some dependency criteria are NOT met. "
                f"Sending record to workcomplete ingest retry queue for {pk}"
            )
            send_message(WCI_RETRY_QUEUE, record, delay_seconds=RETRY_DELAY_SEC)
        elif is_sqs_event:
            # Return batchItemFailures to re-process SQS retry Queue message.
            LOGGER.warning(
                f"All/some dependency criteria are NOT met. "
                f"Re-process workcomplete ingest retry queue for {pk}"
            )
            messages_to_reprocess.append({"itemIdentifier": message_id})
    else:
        # add the work order complete stage flag to workorder summary record
        update_summary_record(pk, sk, upsert_flag_obj)


@with_lambda_profiler(profiling_group_name=PROFILE_GROUP)
@xray_recorder.capture()
def lambda_handler(event: Union[DynamoDBStreamEvent, SQSEvent], _):
    """
    This function process work complete ingest event comes from
    1. work-complete-ingest dynamoDB stream
    2. Retry work complete SQS
    """
    LOGGER.info({"event": event})
    messages_to_reprocess = []
    batch_failure_response = {}

    for record in event["Records"]:
        LOGGER.info({"record": record})
        # Identify the event source
        is_dynamodb_event = record.get("eventSource") == "aws:dynamodb"
        is_sqs_event = record.get("eventSource") == "aws:sqs"
        LOGGER.info(
            {"is_dynamodb_event": is_dynamodb_event, "is_sqs_event": is_sqs_event}
        )
        try:
            charge_call = {}
            event_record_sk = ""
            message_id = ""
            if is_dynamodb_event:
                LOGGER.info("Processing DynamoDB Stream event Record")
                # Decode the dynamodb event record
                decoded_record = j_util.loads(record)
                LOGGER.info({"decoded_record": decoded_record})
                charge_call = decoded_record["dynamodb"]["NewImage"]
                # removed sk for upsert function and created new attribute for retry function
                event_record_sk = charge_call.pop("sk")
                charge_call["event_record_sk"] = event_record_sk
            elif is_sqs_event:
                LOGGER.info("Processing SQS event record")
                # Retrieve the json record for SQS event record
                charge_call = json.loads(record["body"], parse_float=Decimal)
                event_record_sk = charge_call.get("event_record_sk")
                message_id = record.get("messageId", "")
            else:
                # Skip processing the event record
                raise Invalid("Invalid event source record")

            # Process the work complete ingest record
            process_work_complete_record(
                record=charge_call,
                record_sk=event_record_sk,
                is_dynamodb_event=is_dynamodb_event,
                is_sqs_event=is_sqs_event,
                messages_to_reprocess=messages_to_reprocess,
                message_id=message_id,
            )

        except MultipleInvalid as validation_error:
            LOGGER.error(
                {
                    "message": "Record processing will be skipped",
                    "reason": str(validation_error),
                    "record": record,
                }
            )
            record.update({"reason": str(validation_error)})
            send_message(DL_QUEUE, record)

        except ClientError as db_err:
            message = {
                "event": "Client error",
                "reason": str(db_err),
                "record": record,
            }
            LOGGER.error(message)

            record.update({"reason": str(db_err)})
            send_message(DL_QUEUE, record)

        except Exception as err:
            message = {
                "event": "Unknown error",
                "reason": str(err),
                "record": record,
            }
            LOGGER.exception(message)

            record.update({"reason": str(err)})
            send_message(DL_QUEUE, record)

    if messages_to_reprocess:
        LOGGER.error({"List of messages to reprocess": messages_to_reprocess})
        batch_failure_response["batchItemFailures"] = messages_to_reprocess
    return batch_failure_response
