"""
    order retail recon events functions
"""

import json
import base64
import time
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from dynamodb_json import json_util
from botocore.exceptions import ClientError
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid, Any
from environs import Env

from validation import valid_order_retail_recon_estimate
from utils import sqs
from utils.dynamodb import update, remove_item

ENV = Env()

RETRY_QUEUE = ENV("RETRY_QUEUE", None)
DL_QUEUE = ENV("DL_QUEUE", None)
WORKORDER_AM_TABLE = ENV("WORKORDER_AM_TABLE")
DYNAMO = boto3.resource("dynamodb")
RPP_RECON_WORK_ORDER_TABLE = DYNAMO.Table(
    name=ENV("WORKORDER_AM_TABLE", validate=Any(str))
)
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"
RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")

patch_all()


@xray_recorder.capture()
def process_stream(event, _):
    """
    Processing for rpp-order-retailrecon stream events
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

            LOGGER.debug({"decoded record": dynamodb_event})
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

        except ClientError as err:
            handle_client_error(err, record)
        except Exception as err:
            record.update({"reason": str(err)})
            message = {
                "event": "Unknown error",
                "reason": str(err),
                "record": record,
            }

            LOGGER.exception(message)
            sqs.send_message(DL_QUEUE, record)


@xray_recorder.capture()
def process_queue(event, _):
    """
    Processing for rpp-order-retailrecon queue events
    """
    LOGGER.debug({"event": event})

    for record in event["Records"]:
        try:
            record = json.loads(record["body"])
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
        except ClientError as err:
            handle_client_error(err, record, retry_queue=False)
        except Exception as err:
            message = {
                "event": "Unknown error",
                "reason": str(err),
                "record": record,
            }
            LOGGER.exception(message)
            record.update({"reason": str(err)})
            sqs.send_message(DL_QUEUE, record)


def process_record(record):
    """
    process events from either db stream or retry queue if business conditions are met.
    :param record:
    :return:
    """

    try:
        valid_order_retail_recon_estimate(record)

        record_to_update = {}
        if record["eventName"] in ["INSERT", "MODIFY"]:
            payload = record["dynamodb"]["NewImage"]

            pk = 'workorder:' + payload['sblu'] + '#' + payload['site_id']

            active_tasks = payload['order']['activeTasks']
            is_approve_task_exist_in_active = 'Approve' in list(map(lambda x: x['type'], active_tasks))

            completed_tasks = payload['order']['completedTasks']
            is_estimate_task_exist_in_completed = 'Estimate' in list(map(lambda x: x['taskName'], completed_tasks))

            is_pending_rejection_task_exist_in_completed = 'Pending Rejection' in list(map(lambda x: x['taskName'], completed_tasks))
            is_repair_task_exist_in_completed = 'Repair' in list(map(lambda x: x['taskName'], completed_tasks))

            is_approve_task_exist_in_completed = 'Approve' in list(map(lambda x: x['taskName'], completed_tasks))
            is_repair_task_exist_in_active = 'Repair' in list(map(lambda x: x['type'], active_tasks))

            if is_approve_task_exist_in_active and is_estimate_task_exist_in_completed:
                """
                    Get all completedOn fields with 'Estimate' taskName and filter None from them
                    then get latest completed timestamp
                """
                estimate_items = list(filter(lambda x: x['taskName'] == 'Estimate', completed_tasks))
                completed_on_list = list(filter(None, list(map(lambda x: x['completedOn'], estimate_items))))
                completed_timestamp = max(completed_on_list)

                sk = 'estimate_summary' + '#' + completed_timestamp
                key = {
                    "pk": pk,
                    "sk": sk,
                }
                LOGGER.debug({"key": key})

                time.sleep(5)
                entity_type_list = ['fee#', 'labor#', 'part#']
                recon_fee, labor, part = calculate_amount(pk, entity_type_list)

                record_to_update.update({
                    'entity_type': 'estimate_summary',
                    'sblu': payload['sblu'],
                    'site_id': payload['site_id'],
                    'work_order_number': payload['work_order_number'],
                    'reconFee': recon_fee,
                    'labor': labor,
                    'parts': part,
                    'completeTimestamp': completed_timestamp,
                    'updated': Decimal(time.time())
                })

                # VIN is part of index in dynamodb, not insert if empty
                if 'vin' in payload.keys() and payload['vin']:
                    record_to_update.update({
                        'vin': payload['vin']
                    })

                LOGGER.debug({"record to update": record_to_update})
                update(table_name=WORKORDER_AM_TABLE, key=key, update_dict=record_to_update)

            if is_pending_rejection_task_exist_in_completed:

                """
                    Get completedOn field with 'Pending Rejection' taskName
                """
                pending_rejection_items = list(filter(lambda x: x['taskName'] == 'Pending Rejection', completed_tasks))
                completed_on_list = list(filter(None, list(map(lambda x: x['completedOn'], pending_rejection_items))))
                completed_timestamp = max(completed_on_list)
                pending_rejection_completed_moduser = list(filter(lambda x: x['completedOn'] == completed_timestamp, pending_rejection_items))[0]["modUser"]

                sk = 'estimate_summary' + '#' + completed_timestamp
                key = {
                    "pk": pk,
                    "sk": sk,
                }
                LOGGER.debug({"key": key})
                time.sleep(5)

                entity_type_list = ['fee#', 'labor#', 'part#']
                recon_fee, labor, part = calculate_amount(pk, entity_type_list)

                record_to_update.update({
                    'entity_type': 'estimate_summary',
                    'sblu': payload['sblu'],
                    'site_id': payload['site_id'],
                    'work_order_number': payload['work_order_number'],
                    'reconFee': recon_fee,
                    'labor': labor,
                    'parts': part,
                    'completeTimestamp': completed_timestamp,
                    'updated': Decimal(time.time())
                })

                # VIN is part of index in dynamodb, not insert if empty
                if 'vin' in payload.keys() and payload['vin']:
                    record_to_update.update({
                        'vin': payload['vin']
                    })

                LOGGER.debug({"record to update": record_to_update})
                update(table_name=WORKORDER_AM_TABLE, key=key, update_dict=record_to_update)

                if is_repair_task_exist_in_completed and is_approve_task_exist_in_completed:
                    # Process to update approval_summary after work_order is rejected
                    time.sleep(5)
                    process_approval_summary_after_rejection(payload, completed_timestamp, pending_rejection_completed_moduser)

            LOGGER.debug({"completed_tasks": completed_tasks})

            if is_repair_task_exist_in_active and is_approve_task_exist_in_completed:
                approve_items = list(filter(lambda x: x['taskName'] == 'Approve', completed_tasks))
                completed_on_list = list(filter(None, list(map(lambda x: x['completedOn'], approve_items))))
                completed_timestamp = max(completed_on_list)
                approve_completed_moduser = list(filter(lambda x: x['completedOn'] == completed_timestamp, approve_items))[0]["modUser"]

                time.sleep(5)
                process_approval_summary(payload, completed_timestamp, approve_completed_moduser)

        elif record["eventName"] == "REMOVE":
            payload = record["dynamodb"]["OldImage"]
            pk = 'workorder:' + payload['sblu'] + '#' + payload['site_id']

            active_tasks = payload['order']['activeTasks']
            is_approve_task_exist_in_active = 'Approve' in list(map(lambda x: x['type'], active_tasks))

            completed_tasks = payload['order']['completedTasks']
            is_estimate_task_exist_in_completed = 'Estimate' in list(map(lambda x: x['taskName'], completed_tasks))

            if is_approve_task_exist_in_active and is_estimate_task_exist_in_completed:
                estimate_items = list(filter(lambda x: x['taskName'] == 'Estimate', completed_tasks))
                completed_on_list = list(filter(None, list(map(lambda x: x['completedOn'], estimate_items))))
                completed_timestamp = max(completed_on_list)

                sk = 'estimate_summary' + '#' + completed_timestamp
                key = {
                    "pk": pk,
                    "sk": sk,
                }
                remove_item(table_name=WORKORDER_AM_TABLE, key=key)

    except MultipleInvalid as validation_error:
        LOGGER.warning(
            {
                "message": "Record processing will be skipped",
                "reason": str(validation_error),
                "record": record,
            }
        )


def calculate_amount(pk, entity_type_list):
    recon_fee, labor, part = 0, 0, 0

    for entity_type in entity_type_list:
        key_condition_expression = Key('pk').eq(pk) & Key('sk').begins_with(entity_type)
        db_response = RPP_RECON_WORK_ORDER_TABLE.query(
            KeyConditionExpression=key_condition_expression
        )
        data = db_response['Items']
        LOGGER.info({f"db data {entity_type}": data})
        filtered_hidden_data = []
        for item in data:
            if 'hidden' in item and item['hidden'] == 'Y':
                pass
            else:
                filtered_hidden_data.append(item)

        LOGGER.info({"estimated filtered hidden data": filtered_hidden_data})
        if entity_type.startswith('fee') and filtered_hidden_data:
            recon_fee = sum(Decimal(isfloatORint(fee['total_estimate'])) for fee in filtered_hidden_data if
                            'total_estimate' in fee and isNotBlank(str(fee['total_estimate'])))
        elif entity_type.startswith('labor') and filtered_hidden_data:
            labor = sum(Decimal(isfloatORint(labor['extended_price'])) for labor in filtered_hidden_data if
                        'extended_price' in labor and isNotBlank(str(labor['extended_price'])))
        elif entity_type.startswith('part') and filtered_hidden_data:
            part = sum(Decimal(isfloatORint(part['extended_price'])) for part in filtered_hidden_data if
                       'extended_price' in part and isNotBlank(str(part['extended_price'])))
    return recon_fee, labor, part


def handle_client_error(err, record, retry_queue=True):
    error_code = err.response["Error"]["Code"]
    message = {error_code: {"reason": str(err)}}
    reason = err

    LOGGER.error({"event": message, "reason": reason, "record": record})

    if error_code in RETRY_EXCEPTIONS:
        message[error_code].update({"record": record, "event": "Retry Error"})
        LOGGER.warning(message)
        record.update({"reason": str(err)})
        if retry_queue:
            sqs.send_message(RETRY_QUEUE, record)
        else:
            raise err

    elif error_code in IGNORE_EXCEPTIONS:
        message[error_code].update(
            {
                "event": "Ignore error",
                "record": record,
            }
        )
        LOGGER.warning(message)
    else:
        record.update({"reason": str(err)})
        message[error_code].update(
            {
                "event": "Unknown error",
                "record": record,
            }
        )
        LOGGER.exception(message)
        sqs.send_message(DL_QUEUE, record)


def calculate_approve_summary_amount(pk, entity_type_list):
    recon_fee, labor, part = 0, 0, 0

    for entity_type in entity_type_list:
        key_condition_expression = Key('pk').eq(pk) & Key('sk').begins_with(entity_type)
        db_response = RPP_RECON_WORK_ORDER_TABLE.query(
            KeyConditionExpression=key_condition_expression
        )
        data = db_response['Items']
        filtered_hidden_data = []
        LOGGER.info({f"db data {entity_type}": data})
        for item in data:
            if 'hidden' in item and item['hidden'] == 'Y':
                pass
            elif 'skipped' in item and item['skipped'] == 'Y':
                pass
            elif 'approved' in item and item['approved'] == 'Y':
                filtered_hidden_data.append(item)

        LOGGER.info({"approved filtered_hidden_data": filtered_hidden_data})
        if entity_type.startswith('fee') and filtered_hidden_data:
            recon_fee = sum(Decimal(isfloatORint(fee['total_estimate'])) for fee in filtered_hidden_data if
                            'total_estimate' in fee and isNotBlank(str(fee['total_estimate'])))
        elif entity_type.startswith('labor') and filtered_hidden_data:
            labor = sum(Decimal(isfloatORint(labor['extended_price'])) for labor in filtered_hidden_data if
                        'extended_price' in labor and isNotBlank(str(labor['extended_price'])))
        elif entity_type.startswith('part') and filtered_hidden_data:
            part = sum(Decimal(isfloatORint(part['extended_price'])) for part in filtered_hidden_data if
                       'extended_price' in part and isNotBlank(str(part['extended_price'])))
    return recon_fee, labor, part


def process_approval_summary(payload, completed_time, approve_completed_moduser):
    record_to_update = {}
    completed_timestamp = get_current_timestamp()
    pk = 'workorder:' + payload['sblu'] + '#' + payload['site_id']
    approve_summary_record = get_approval_summary_record(pk, 'approve_summary#')
    LOGGER.info({"approve_summary_record": approve_summary_record})

    entity_type_list = ['fee#', 'labor#', 'part#']
    recon_fee, labor, part = calculate_approve_summary_amount(pk, entity_type_list)
    LOGGER.info({'recon_fee': recon_fee, 'labor': labor, 'part': part})

    # If labor = 0 and there's no approve_summary record
    if labor == 0 and not approve_summary_record:
        sk = 'approve_summary' + '#' + completed_timestamp
        key = {
            "pk": pk,
            "sk": sk,
        }
        updatedBy = None
        if 'updatedBy' in payload['order']:
            updatedBy = payload['order']['updatedBy']
        else:
            updatedBy = approve_completed_moduser

        record_to_update.update({
            'entity_type': 'approve_summary',
            'reconFee': recon_fee,
            'labor': labor,
            'parts': part,
            'sblu': payload['sblu'],
            'site_id': payload['site_id'],
            'work_order_number': payload['work_order_number'],
            'completeTimestamp': completed_time,
            'approverUserName': updatedBy,
            'updated': Decimal(time.time())
        })

        # VIN is part of index in dynamodb, not insert if empty
        if 'vin' in payload.keys() and payload['vin']:
            record_to_update.update({
                'vin': payload['vin']
            })
        LOGGER.info({"approve_summary record to update": record_to_update})
        update(table_name=WORKORDER_AM_TABLE, key=key, update_dict=record_to_update)


def process_approval_summary_after_rejection(payload, completed_timestamp, pending_rejection_completed_moduser):

    pk = 'workorder:' + payload['sblu'] + '#' + payload['site_id']
    latest_approve_summary = get_approval_summary_record(pk, 'approve_summary#')
    LOGGER.info({"latest_approve_summary": latest_approve_summary})

    if latest_approve_summary:

        entity_type_list = ['fee#', 'labor#', 'part#']
        recon_fee, labor, part = calculate_approve_summary_amount(pk, entity_type_list)
        LOGGER.info({'recon_fee': recon_fee, 'labor': labor, 'part': part})

        key = {
            "pk": latest_approve_summary["pk"],
            "sk": 'approve_summary' + '#' + completed_timestamp
        }

        record_to_update = {}
        updatedBy = None
        if 'updatedBy' in payload['order']:
            updatedBy = payload['order']['updatedBy']
        else:
            updatedBy = pending_rejection_completed_moduser

        record_to_update.update({
            'entity_type': 'approve_summary',
            'reconFee': recon_fee,
            'labor': labor,
            'parts': part,
            'sblu': payload['sblu'],
            'site_id': payload['site_id'],
            'work_order_number': payload['work_order_number'],
            'completeTimestamp': completed_timestamp,
            'approverUserName': updatedBy,
            'updated': Decimal(time.time())
        })

        # VIN is part of index in dynamodb, not insert if empty
        if 'vin' in payload.keys() and payload['vin']:
            record_to_update.update({
                'vin': payload['vin']
            })

        LOGGER.info({"approve_summary record to update": record_to_update})
        update(table_name=WORKORDER_AM_TABLE, key=key, update_dict=record_to_update)


def isNotBlank(price):
    if price and price.strip():
        return True
    return False


def isfloatORint(num):
    try:
        float(num)
        return num
    except ValueError:
        return 0


def get_approval_summary_record(pk, sk):
    """
        return approve_summary record of a given workorder
        return {} if there's no approve_summary record
    """

    approve_summary = {}
    key_condition_expression = Key("pk").eq(pk) & Key(
        "sk"
    ).begins_with(sk)

    query_result = RPP_RECON_WORK_ORDER_TABLE.query(
        KeyConditionExpression=key_condition_expression,
        ScanIndexForward=False,
    ).get("Items", [])

    if query_result:
        approve_summary = query_result[0]

    return approve_summary


def get_current_timestamp():
    from datetime import datetime
    str_date_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(str_date_time)
