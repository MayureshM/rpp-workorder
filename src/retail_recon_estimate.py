"""
    retail recon estimate events functions
"""

import json
import base64
import time
from decimal import Decimal
from dynamodb_json import json_util
from botocore.exceptions import ClientError
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid, Any
from environs import Env

from validation import valid_recon_retail_estimate
from utils import sqs
from utils.dynamodb import update, remove_item, delete_field_item
import boto3
from boto3.dynamodb.conditions import Key

ENV = Env()

RETRY_QUEUE = ENV("RETRY_QUEUE", None)
DL_QUEUE = ENV("DL_QUEUE", None)
WORKORDER_AM_TABLE = ENV("WORKORDER_AM_TABLE")
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"
RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")
DYNAMO = boto3.resource("dynamodb")
RPP_RECON_WORK_ORDER_TABLE = DYNAMO.Table(name=ENV("WORKORDER_AM_TABLE", validate=Any(str)))
patch_all()


@xray_recorder.capture()
def process_stream(event, _):
    """
    Processing for rpp-re-ingest stream events
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
            LOGGER.info(message)
            sqs.send_message(DL_QUEUE, record)


@xray_recorder.capture()
def process_queue(event, _):
    """
    Processing for rpp-re-ingest queue events
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
        valid_recon_retail_estimate(record)

        if record["eventName"] in ["INSERT", "MODIFY"]:
            payload = record["dynamodb"]["NewImage"]
            OldImage = None
            if 'OldImage' in record["dynamodb"]:
                OldImage = record["dynamodb"]["OldImage"]

            # Get entity_type from payload
            entity_type = payload['sk'].split('#')[0]
            if 'damage' in entity_type:
                payload.update({
                    'entity_type': 'damage'
                })
            else:
                payload.update({
                    'entity_type': entity_type
                })

            pk = 'workorder:' + payload['sblu'] + '#' + payload['site_id']
            sk = payload['sk']
            key = {
                "pk": pk,
                "sk": sk,
            }
            payload.pop("sk")
            payload.pop("pk")
            payload['updated'] = Decimal(time.time())
            # VIN is part of index in dynamodb, remove from payload if empty
            if 'vin' in payload.keys() and not payload['vin']:
                payload.pop("vin")

            if OldImage and ('eta' in OldImage) and \
                    ('eta' in payload.keys() and (payload['eta'] == "" or payload['eta'] is None)):
                delete_field_item(table_name=WORKORDER_AM_TABLE, key=key, attribute='eta')

            if 'eta' in payload.keys() and (payload['eta'] == '' or payload['eta'] is None):
                payload.pop("eta")

            LOGGER.debug({"payload to update": payload})
            update(table_name=WORKORDER_AM_TABLE, key=key, update_dict=payload)
            process_approval_summary(payload, OldImage)
        elif record["eventName"] == "REMOVE":
            payload = record["dynamodb"]["OldImage"]

            pk = 'workorder:' + payload['sblu'] + '#' + payload['site_id']
            sk = payload['sk']
            key = {
                "pk": pk,
                "sk": sk,
            }
            payload.pop("sk")
            payload.pop("pk")
            remove_item(table_name=WORKORDER_AM_TABLE, key=key)

    except MultipleInvalid as validation_error:
        LOGGER.warning(
            {
                "message": "Record processing will be skipped",
                "reason": str(validation_error),
                "record": record,
            }
        )


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


def process_approval_summary(newImage, oldImage):
    try:
        LOGGER.debug({"newImage": newImage})
        if oldImage:
            LOGGER.debug({"Approve_summary_record_key": oldImage})
            sk = oldImage.get('sk', None)
            if sk is None:
                sk = newImage.get('entity_type', None)
            if sk and 'fee' in sk and ('skipped' in newImage or 'skipped' in oldImage):
                if newImage.get("skipped", None) != oldImage.get("skipped", None):
                    record_to_update = {}
                    completed_timestamp = get_current_timestamp()

                    pk = 'workorder:' + newImage['sblu'] + '#' + newImage['site_id']
                    entity_type_list = ['fee#', 'labor#', 'part#']
                    recon_fee, labor, part = calculate_approve_summary_amount(pk, entity_type_list)
                    sk = 'approve_summary' + '#' + completed_timestamp
                    key = {
                        "pk": pk,
                        "sk": sk,
                    }
                    LOGGER.debug({"Approve summary record key": key})
                    updatedBy = None
                    if 'mod_user' in newImage:
                        updatedBy = newImage['mod_user']
                    record_to_update.update({
                        'entity_type': 'approve_summary',
                        'reconFee': recon_fee,
                        'labor': labor,
                        'parts': part,
                        'sblu': newImage['sblu'],
                        'site_id': newImage['site_id'],
                        'work_order_number': newImage['work_order_number'],
                        'completeTimestamp': completed_timestamp,
                        'approverUserName': updatedBy,
                        'updated': Decimal(time.time())
                    })
                    # VIN is part of index in dynamodb, not insert if empty
                    if 'vin' in newImage.keys() and newImage['vin']:
                        record_to_update.update({
                            'vin': newImage['vin']
                        })
                    LOGGER.debug({"approve_summary record to update": record_to_update})
                    update(table_name=WORKORDER_AM_TABLE, key=key, update_dict=record_to_update)
    except Exception as e:
        LOGGER.debug(e)


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


def get_current_timestamp():
    from datetime import datetime
    str_date_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(str_date_time)


def calculate_approve_summary_amount(pk, entity_type_list):
    recon_fee, labor, part = 0, 0, 0

    for entity_type in entity_type_list:
        key_condition_expression = Key('pk').eq(pk) & Key('sk').begins_with(entity_type)
        db_response = RPP_RECON_WORK_ORDER_TABLE.query(
            KeyConditionExpression=key_condition_expression
        )
        data = db_response['Items']
        filtered_hidden_data = []
        LOGGER.debug({f"db data {entity_type}": data})
        for item in data:
            if 'hidden' in item and item['hidden'] == 'Y':
                pass
            elif 'skipped' in item and item['skipped'] == 'Y':
                pass
            elif 'approved' in item and item['approved'] == 'Y':
                filtered_hidden_data.append(item)

        LOGGER.debug({"approved filtered_hidden_data": filtered_hidden_data})
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
