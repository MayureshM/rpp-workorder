"""
    order retailrecon functions
"""

import boto3
import json
import time
from decimal import Decimal
import stringcase

from environs import Env
from voluptuous import MultipleInvalid
from voluptuous import Any
from rpp_lib.logs import LOGGER
from validation import valid_retailrecon_completed
from validation import valid_retailrecon_updated
from dynamodb.store import put_work_order
from rpp_lib.rpc import get_pfvehicle
from utils.common import get_vin
from dynamodb.store import delete_record, get_all_by_pk

ENV = Env()
DYNAMO = boto3.resource('dynamodb')
CATEGORY_TABLE = DYNAMO.Table(name=ENV('CATEGORY_TABLE', validate=Any(str)))


def build_order(new_record):
    """
        build retailrecon order dictionary
    """

    order = None
    item = CATEGORY_TABLE.get_item(Key={"key": "UCFIN"})["Item"]
    LOGGER.debug({"item_ucfin": item})
    retailrecon_service = {
        "retailrecon_id": new_record["retailrecon_id"],
        "shop": item["category"],
        "shop_description": item["shopDescription"],
        "labor_hours": item["hours"],
        "labor_name": item["laborName"],
        "product_id": item["productId"],
        "labor_status": "Ready for Repair"
    }
    LOGGER.debug({"retailrecon_service": retailrecon_service})

    try:
        order = valid_retailrecon_updated(new_record["order"])
        order.pop("consignment")
        order.update(retailrecon_service)
        order.update(
            {
                "inspection_stage": order["activeTasks"][0]["type"]
            }
        )

    except MultipleInvalid as validation_error:
        message = {
            "message": "Not retailrecon updated, trying completed",
            "reason": str(validation_error),
            "event": new_record
        }

        LOGGER.warning(message)
        try:
            order = valid_retailrecon_completed(new_record["order"])
            order.pop("consignment")
            order.update(retailrecon_service)
            order.update({"labor_status": "Completed"})
            order.update({"request_date": order["createdOn"]})
            complete_date = [
                task for task in order["completedTasks"] if task[
                    "taskName"] == "Mechanical Inspection"][0]["completedOn"]
            order.update({"complete_date": complete_date})

        except MultipleInvalid as validation_error:
            message = {
                "message": "Ignoring retailrecon event",
                "reason": str(validation_error),
                "event": new_record
            }

            LOGGER.warning(message)

    return order


def get_order_retailrecon(new_image):
    """
        return order retailrecon column
    """

    column = None
    try:
        data = build_order(new_image)
        if data is not None:
            column = {
                "name": "ucfin",
                "data": data
            }
    except KeyError as k_error:
        message = {
            "message": "Bad retailrecon event",
            "reason": str(k_error),
            "event": new_image
        }
        LOGGER.warning(message)

    return column


def process_retail_recon(record, wo_key, key_event):
    LOGGER.debug({"Record received": record})
    entity_type = "retail_recon"
    completed_tasks = record['order'].pop('completedTasks')
    active_tasks = record['order'].pop('activeTasks')

    work_order_number = record.get('work_order_number')
    vin = record.get('vin')

    if not work_order_number or not vin:
        #  get pfvehicle record for work_order_key
        pfvehicle = get_pfvehicle(work_order_key=wo_key)
        LOGGER.debug({
            "pfvehicle": pfvehicle
        })
        if pfvehicle:
            pfvehicle = json.loads(pfvehicle)
            work_order_number = pfvehicle['work_order_number']
            vin = get_vin(pfvehicle['pfvehicle'])

    general_record_data = {
        "sblu": record['sblu'],
        "site_id": record['site_id'],
        "work_order_number": work_order_number,
        "vin": vin,
        "entity_type": entity_type,
        "updated": Decimal(time.time())
    }

    general_record_data.update({
        stringcase.snakecase(k): v for k, v in
        record["order"].items()
    })
    general_record_data.update({"key_src": key_event})
    LOGGER.debug({"General record": general_record_data})
    put_work_order(wo_key, entity_type, general_record_data)

    if general_record_data.get('rejected', False):
        record_data = {
            "rejected": general_record_data.get('rejected', False),
            "updated": Decimal(time.time()),
        }
        put_work_order(wo_key, 'workorder:' + wo_key, record_data)

    # completed tasks document
    for completed in completed_tasks:
        completed_task_record = {
            "sblu": record['sblu'],
            "site_id": record['site_id'],
            "work_order_number": work_order_number,
            "vin": vin,
            "entity_type": entity_type,
            "updated": Decimal(time.time())
        }
        completed_task_record.update(completed)

        if completed['completedOn']:
            sk = 'completed_task:%s#%s' % (stringcase.snakecase(
                completed['taskName'].replace(" ", "")), completed['completedOn'])
            put_work_order(wo_key, sk, completed_task_record)

    # active tasks document
    for active in active_tasks:
        active_task_record = {
            "sblu": record['sblu'],
            "site_id": record['site_id'],
            "work_order_number": work_order_number,
            "vin": vin,
            "entity_type": entity_type,
            "updated": Decimal(time.time())
        }
        active_task_record.update(active)

        if active['createdOn']:
            created_date = active['createdOn']
        else:
            created_date = "0000-00-00T00:00:00Z"
        sk = 'active_task:%s#%s' % (stringcase.snakecase(
            active['type'].replace(" ", "")), created_date)

        if sk.startswith("active_task:vehicle_qualification"):
            customer_id = active_task_record.pop("customerId", None)
            if customer_id:
                active_task_record.update({"customer_id": str(customer_id)})

        put_work_order(wo_key, sk, active_task_record)


def delete_work_order(old_image: object):
    LOGGER.info({"Delete work order": old_image})

    wo_key = old_image.get("work_order_key", None)
    active_task_type = "active_task"
    completed_task_type = "completed_task"

    # get work order records
    wo_records = get_all_by_pk(f"workorder:{wo_key}")
    for wo_item in wo_records:
        wo_sk = wo_item["sk"]
        # delete work order record if it is completed_task, active_task or retail_recon
        if wo_sk.startswith(active_task_type) or wo_sk.startswith(completed_task_type) or wo_sk == "retail_recon":
            delete_record(wo_key, wo_sk)
