"""
    order condition functions
"""

import boto3
import json
import time
import stringcase
from decimal import Decimal
from environs import Env
from voluptuous import MultipleInvalid
from voluptuous import Any
from rpp_lib.logs import LOGGER
from rpp_lib.rpc import get_pfvehicle, get_capture
from utils.common import get_vin
from validation import valid_condition_completed, validate_order_condition_schema
from validation import valid_condition_updated
from dynamodb.store import put_work_order, get_work_order, DynamoItemNotFound
from utils.constants import EVENT_SOURCE_SMART_INSPECT, EVENT_SOURCE_AUCTION_ECR, CAPTURE_COMPLETE
from json.decoder import JSONDecodeError

ENV = Env()
DYNAMO = boto3.resource("dynamodb")
CATEGORY_TABLE = DYNAMO.Table(name=ENV("CATEGORY_TABLE", validate=Any(str)))


def build_order(new_record):
    """
    build condition order dictionary
    """

    order = None
    item = CATEGORY_TABLE.get_item(Key={"key": "CR/SO"})["Item"]
    condition_service = {
        "condition_id": new_record["condition_id"],
        "shop": item["category"],
        "shop_description": item["shopDescription"],
        "labor_hours": item["hours"],
        "labor_name": item["laborName"],
        "labor_status": "Ready for Repair",
    }

    try:
        order = valid_condition_updated(new_record["order"])
        order.pop("consignment")
        order.update(condition_service)
        order.update({"request_date": order["createdTimestamp"]})
    except MultipleInvalid:
        try:
            order = valid_condition_completed(new_record["order"])
            order.pop("consignment")
            order.update(condition_service)
            order.update({"labor_status": "Completed"})
            order.update({"request_date": order["createdTimestamp"]})
            order.update({"complete_date": order["completedTimestamp"]})

        except MultipleInvalid as validation_error:
            message = {
                "message": "Ignoring condition event",
                "reason": str(validation_error),
                "event": new_record,
            }

            LOGGER.warning(message)

    return order


def get_order_condition(new_image):
    """
    return order condition column
    """

    column = None
    try:
        data = build_order(new_image)
        if data is not None:
            column = {"name": "condition_service", "data": data}
    except KeyError as k_error:
        message = {
            "message": "Bad condition event",
            "reason": str(k_error),
            "event": new_image,
        }
        LOGGER.warning(message)

    return column


def process_condition(record, wo_key, key_event, entity_type):
    LOGGER.debug("Processing condition")
    tires = record.get("order", {}).get("condition", {}).get("tires", [])
    work_order_number = record.get("work_order_number")
    vin = record.get("vin")

    if not work_order_number or not vin:
        #  get pfvehicle record for work_order_key
        pfvehicle = get_pfvehicle(work_order_key=wo_key)
        LOGGER.debug({"pfvehicle": pfvehicle})
        if pfvehicle:
            pfvehicle = json.loads(pfvehicle)
            work_order_number = pfvehicle["work_order_number"]
            vin = get_vin(pfvehicle["pfvehicle"])
    general_record_data = {
        "sblu": record["sblu"],
        "site_id": record["site_id"],
        "work_order_number": work_order_number,
        "vin": vin,
        "entity_type": entity_type,
        "updated": Decimal(time.time()),
    }
    general_record_data.update(
        {stringcase.snakecase(k): v for k, v in record["order"].items()}
    )
    general_record_data.update({"key_src": key_event})

    put_work_order(wo_key, entity_type, general_record_data)
    LOGGER.debug({"tires": tires})
    for tire in tires:
        tire_record_data = {
            "sblu": record["sblu"],
            "site_id": record["site_id"],
            "work_order_number": work_order_number,
            "vin": vin,
            "entity_type": "tire",
            "updated": Decimal(time.time()),
            "source": "ECR",
            "is_estimate_assistant": "N",
        }
        tire_record_data.update({stringcase.snakecase(k): v for k, v in tire.items()})
        sk = "tire:%s" % (stringcase.snakecase(tire["location"].lower()))
        try:
            current_tire = get_work_order(pk=f"workorder:{wo_key}", sk=sk)
            LOGGER.debug({"Current tire": current_tire})
        except DynamoItemNotFound:
            put_work_order(wo_key, sk, tire_record_data)


def add_condition_data_summary(new_image):
    """
    Function that will add order information to the given
    record matching the document_name.
    """
    try:
        record = validate_order_condition_schema(new_image)
    except MultipleInvalid as v_error:
        LOGGER.exception(
            {
                "type": "MultipleInvalid",
                "error": str(v_error),
                "new_image": new_image,
            }
        )
        return

    wo_key = record["work_order_key"]

    sblu = record.get("sblu", None)
    vin = record.get("vin", None)
    site_id = record.get("site_id", None)
    work_order_number = record.get("work_order_number", None)
    if not sblu or not vin or not site_id or not work_order_number:
        pfvehicle = json.loads(get_pfvehicle(work_order_key=wo_key))

        if not sblu and pfvehicle:
            sblu = pfvehicle["sblu"]
        if not vin and pfvehicle:
            pfvehicle_vin = get_vin(pfvehicle("pfvehicle"))
            if pfvehicle_vin:
                vin = pfvehicle_vin
        if not site_id and pfvehicle:
            site_id = pfvehicle["site_id"]
        if not work_order_number and pfvehicle:
            work_order_number = pfvehicle["work_order_number"]
    order_data = {}
    order_data.update({stringcase.snakecase(k): v for k, v in record["order"].items()})
    record_data = {
        "inspection_platform": record.get("order", {})
        .get("condition", {})
        .get("inspectionPlatform", None),
        "condition_status": order_data.get("status", None),
        "condition_completed_timestamp": record.get("order", {}).get(
            "completedTimestamp", None
        ),
        "condition_updated": Decimal(time.time()),
        "sblu": sblu,
        "entity_type": "summary",
        "vin": vin,
        "site_id": site_id,
        "work_order_number": work_order_number,
        "updated": Decimal(time.time()),
    }
    # We need to override order.condition.completedDate with capture_completed_timestamp only if
    # both of the following conditions are True:
    # 1. capture_exists is False
    # 2. inspection_platform is either Smart_Inspect or Auction_ECR
    capture_record_data = {}
    inspection_platform = record_data.get("inspection_platform", None)
    capture_exists = False
    if inspection_platform == EVENT_SOURCE_AUCTION_ECR:
        LOGGER.info({
            "message": "Checking if capture exists",
            "work_order_key": wo_key,

        })
        try:
            get_capture_response = get_capture(capture_id=None, work_order_key=wo_key, func_arn=None)
            LOGGER.info({
                "message": "Get Capture response",
                "response": get_capture_response,
            })
        except JSONDecodeError as exc:
            LOGGER.exception({
                "message": "Error decoding get_capture_response",
                "error": exc,
            })
        capture_exists = len(get_capture_response) > 0
    if not capture_exists and inspection_platform in [EVENT_SOURCE_SMART_INSPECT, EVENT_SOURCE_AUCTION_ECR]:
        capture_record_data = {
            "capture_completed_timestamp": record.get("order", {}).get("condition", {}).get("completedDate", None),
            "capture_status": CAPTURE_COMPLETE,
            "capture_updated": Decimal(time.time()),

        }
    record_data.update(capture_record_data)

    sk = f"workorder:{wo_key}"
    put_work_order(wo_key, sk, record_data)
