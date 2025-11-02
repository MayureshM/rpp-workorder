"""
   order detail functions
"""
import boto3
from environs import Env
from voluptuous import MultipleInvalid
from voluptuous import Any
from rpp_lib.logs import LOGGER
from validation import valid_detail_completed
from validation import valid_detail_requested
from validation import valid_detail_declined
from validation import valid_detail_canceled


ENV = Env()
DYNAMO = boto3.resource('dynamodb')
CATEGORY_TABLE = DYNAMO.Table(name=ENV('CATEGORY_TABLE', validate=Any(str)))


def get_category_data(key):
    """
        get detail category info from category table
    """
    item = CATEGORY_TABLE.get_item(Key={"key": key})["Item"]
    result = {
        "shop": item["category"],
        "shop_description": item["shopDescription"],
        "labor_hours": item["hours"],
        "labor_name": item["laborName"]
    }

    if "productId" in item:
        result["product_id"] = item["productId"]

    return result


def build_order(new_record):
    """
        build detail order dictionary
    """

    order = None
    new_record["order"].pop("consignment")
    detail_service = {
        "detail_id": new_record["detail_id"],
        "labor_status": "Ready for Repair"
    }

    try:
        order = valid_detail_requested(new_record["order"])
        key = "DETL#" + order["type"]
        detail_service.update(get_category_data(key))
        detail_service.update({"date_requested": order["createdOn"]})
        order.update(detail_service)

    except MultipleInvalid as validation_error:
        message = {
            "message": "Not a detail requested event",
            "reason": str(validation_error),
            "event": new_record
        }
        LOGGER.debug(message)

        try:
            order = valid_detail_declined(new_record["order"])
            key = "DETL#" + order["type"]
            detail_service.update(get_category_data(key))
            detail_service['labor_status'] = "Declined"
            detail_service.update(
                {"date_requested": order["createdOn"]})
            detail_service.update(
                {"date_completed": order["updatedOn"]})
            order.update(detail_service)

        except MultipleInvalid as validation_error:
            message = {
                "message": "Not a detail Declined event",
                "reason": str(validation_error),
                "event": new_record
            }
            LOGGER.debug(message)

            try:
                order = valid_detail_canceled(new_record["order"])
                key = "DETL#" + order["type"]
                detail_service.update(get_category_data(key))
                detail_service['labor_status'] = "Rejected"
                detail_service.update(
                    {"date_requested": order["createdOn"]})
                detail_service.update(
                    {"date_completed": order["updatedOn"]})
                order.update(detail_service)

            except MultipleInvalid as validation_error:
                message = {
                    "message": "Not a detail canceled event",
                    "reason": str(validation_error),
                    "event": new_record
                }
                LOGGER.debug(message)

                try:
                    order = valid_detail_completed(new_record["order"])
                    key = "DETL#" + order["type"]
                    detail_service.update(get_category_data(key))
                    detail_service['labor_status'] = "Completed"
                    detail_service.update(
                        {"date_requested": order["createdOn"]})
                    detail_service.update(
                        {"date_completed": order["updatedOn"]})
                    order.update(detail_service)

                except MultipleInvalid as validation_error:
                    message = {
                        "message": "Ignoring detail event, did not match any \
                        known detail event types",
                        "reason": str(validation_error),
                        "event": new_record
                    }

            LOGGER.warning(message)

    return order


def get_order_detail(new_image):
    """
        return order detail column
    """

    column = None
    try:
        data = build_order(new_image)
        if data is not None:
            column = {
                "name": "detl",
                "data": data
            }
    except KeyError as k_error:
        message = {
            "message": "Bad detail event",
            "reason": str(k_error),
            "event": new_image
        }
        LOGGER.warning(message)

    return column
