""" handler for find-work-order lambda function """
import json

import boto3
import simplejson as simple_json
from boto3.dynamodb.conditions import Key
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import Any, MultipleInvalid

from validation import validate_work_order_request

ENV = Env()

DYNAMO = boto3.resource("dynamodb")
WORKORDER_TABLE = DYNAMO.Table(name=ENV("WORKORDER_TABLE", validate=Any(str)))

HEADERS = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*'
}

IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"


def find_work_order(event, _):
    """Retrieves a work order record from rpp-workorder table.

    Attempts to find the work order via it's GSI and if the data is available
    otherwise it makes a get_item request by the work_order_key.

    Arguments:
        event {dict} -- The event from the lambda invocation.
    """

    LOGGER.debug({"event": event})

    work_order = None

    try:

        try:
            work_order_key = event["work_order_key"]
        except KeyError:
            work_order_key = None

        if work_order_key is not None:
            site_id = work_order_key.split("#")[1]
            key = {"work_order_key": work_order_key, "site_id": site_id}
            db_response = WORKORDER_TABLE.get_item(Key=key)
            try:
                work_order = db_response["Item"]
            except KeyError:
                work_order = []
        else:
            request = validate_work_order_request(event)
            work_order_number = request["work_order_number"]
            site_id = request["site_id"]
            key_expression = Key("work_order_number").eq(work_order_number) & Key(
                "site_id"
            ).eq(site_id)

            db_response = WORKORDER_TABLE.query(
                IndexName="index_work_order_number",
                KeyConditionExpression=key_expression,
            )

            # can't extract item from list since this was existing code being used
            work_order = None or db_response.get("Items")

    except MultipleInvalid as error:
        message = {
            "message": "Invalid work order request",
            "event": event,
            "error": str(error),
        }
        LOGGER.error(message)

        raise Exception(json.dumps(message))

    return work_order


def get_work_order(event, _):
    LOGGER.debug(event)
    query_params = event.get('queryStringParameters') or {}

    params = {
        "site_id": query_params.get('siteId'),
        "work_order_key": query_params.get('workOrderKey'),
        "work_order_number": query_params.get('workOrderNumber')
    }

    try:
        body = find_work_order(params, None) or []
        status_code = 200
        return {
            'statusCode': status_code,
            'headers': HEADERS,
            'body': simple_json.dumps(body)
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'headers': HEADERS,
            'body': json.dumps({
                "error": str(e)
            })
        }
