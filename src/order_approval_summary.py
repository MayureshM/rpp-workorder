"""
    order approval events functions
"""

import time
from decimal import Decimal
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
from rpp_lib.logs import LOGGER
from voluptuous import Any
from environs import Env

import boto3
from boto3.dynamodb.conditions import Key
from utils.dynamodb import update

ENV = Env()

WORKORDER_AM_TABLE = ENV("WORKORDER_AM_TABLE")
DYNAMO = boto3.resource("dynamodb")
RPP_RECON_WORK_ORDER_TABLE = DYNAMO.Table(
    name=ENV("WORKORDER_AM_TABLE", validate=Any(str))
)

patch_all()


@xray_recorder.capture()
def process_approval_summary(event):
    record_to_update = {}
    completed_timestamp = event['order']['updatedOn']

    pk = 'workorder:' + event['sblu'] + '#' + event['site_id']
    sk = 'approve_summary' + '#' + completed_timestamp

    entity_type_list = ['fee#', 'labor#', 'part#']
    recon_fee, labor, part = calculate_amount(pk, entity_type_list)

    key = {
        "pk": pk,
        "sk": sk,
    }
    LOGGER.debug({"key": key})

    record_to_update.update({
        'entity_type': 'approve_summary',
        'reconFee': recon_fee,
        'labor': labor,
        'parts': part,
        'sblu': event['sblu'],
        'site_id': event['site_id'],
        'work_order_number': event['work_order_number'],
        'completeTimestamp': completed_timestamp,
        'approverUserName': event['order']['updatedBy'],
        'updated': Decimal(time.time())
    })

    # VIN is part of index in dynamodb, not insert if empty
    if 'vin' in event.keys() and event['vin']:
        record_to_update.update({
            'vin': event['vin']
        })

    LOGGER.debug({"record to update": record_to_update})
    update(table_name=WORKORDER_AM_TABLE, key=key, update_dict=record_to_update)


def calculate_amount(pk, entity_type_list):
    recon_fee, labor, part = 0, 0, 0

    for entity_type in entity_type_list:
        key_condition_expression = Key('pk').eq(pk) & Key('sk').begins_with(entity_type)
        db_response = RPP_RECON_WORK_ORDER_TABLE.query(
            KeyConditionExpression=key_condition_expression
        )
        data = db_response['Items']
        filtered_hidden_data = []
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
