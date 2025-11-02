# pylint: disable=missing-docstring
import boto3
from boto3.dynamodb.conditions import Key
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import Any, MultipleInvalid

from validation import valid_labor_category_request

ENV = Env()
DYNAMO = boto3.resource("dynamodb")
LABOR_CATEGORY_TABLE = DYNAMO.Table(name=ENV("LABOR_CATEGORY_TABLE", validate=Any(str)))


def find_labor_category(event, _):
    LOGGER.debug({"event": event})

    response = {}
    labor_category = {}

    try:

        request = valid_labor_category_request(event)

        if request["key"]:
            response = LABOR_CATEGORY_TABLE.get_item(Key={"key": request["key"]})
            labor_category = response.get("Item", {})

        elif request["category"]:
            response = LABOR_CATEGORY_TABLE.query(
                IndexName="category_index",
                KeyConditionExpression=Key("category").eq(request["category"]),
            )
            labor_category = response.get("Items", [])

        elif request["product_id"]:
            response = LABOR_CATEGORY_TABLE.query(
                IndexName="productId_index",
                KeyConditionExpression=Key("productId").eq(request["product_id"]),
            )
            labor_category = response.get("Items", [])

        elif request["shop_description"]:
            response = LABOR_CATEGORY_TABLE.query(
                IndexName="shopDescription_index",
                KeyConditionExpression=Key("shopDescription").eq(
                    request["shop_description"]
                ),
            )
            labor_category = response.get("Items", [])

    except (KeyError, MultipleInvalid) as error:
        LOGGER.warning(
            {
                "message": "Returning empty list",
                "request": request if request else "",
                "db_response": response if response else "",
                "labor_category": labor_category,
                "error": str(error),
            }
        )

    return labor_category
