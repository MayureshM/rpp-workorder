from operator import itemgetter
from dynamodb.store import get_work_order, get_work_oder_index, query, DynamoItemNotFound
from validator.recon_work_order import (
    validate_find_work_order, validate_get_conditions_by_vin, validate_damage,
    validate_condition, validate_workorder_damage, validate_tire, validate_primary_key
)
from aws_xray_sdk.core import patch_all, xray_recorder
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid
from boto3.dynamodb.conditions import Attr, Key
from utils.dynamodb import get_response, get_error, HEADERS
import simplejson as s_json
import json
from http import HTTPStatus


patch_all()


@xray_recorder.capture("find")
def find(event, _):
    LOGGER.debug({"event": event})
    response = None

    try:
        request_params = validate_find_work_order(event)

        if request_params.get("index"):
            response = get_work_oder_index(
                request_params["key"],
                request_params["index"]
            )

        else:
            response = get_work_order(
                request_params["key"]["pk"],
                request_params["key"]["sk"]
            )
            response.update(request_params["key"])

    except MultipleInvalid as v_error:
        LOGGER.warn(
            {
                "type": "MultipleInvalid",
                "error": str(v_error),
                "event": event,
            }
        )

    except KeyError as k_error:
        LOGGER.warn(
            {
                "type": "MultipleInvalid",
                "error": str(k_error),
                "event": event,
            }
        )

    except DynamoItemNotFound as d_error:
        LOGGER.warn(
            {
                "type": "MultipleInvalid",
                "error": str(d_error),
                "event": event,
            }
        )

    except Exception as e_error:
        LOGGER.warn(
            {
                "type": "Exception",
                "error": str(e_error),
                "event": event,
            }
        )

    LOGGER.debug({"response": response})
    return response


def get_conditions_by_vin(event, _):
    LOGGER.debug({"event": event})
    damages = []
    tires = []

    try:
        request_params = validate_get_conditions_by_vin(event)
        vin = request_params["vin"]

        key_condition_expression = Key("vin").eq(vin) & \
            Key("sk").begins_with("conditions")

        args = {
            "IndexName": "index_vin",
            "KeyConditionExpression": key_condition_expression
        }

        if request_params.get("site_id") and request_params.get("work_order_number"):
            filter_exp = Attr("site_id").eq(request_params.get("site_id")) & Attr("work_order_number").eq(request_params.get("work_order_number"))
            args["FilterExpression"] = filter_exp

        response = query(args)

        if not response.get("Items"):
            raise DynamoItemNotFound(
                404,
                f"No records found for VIN:{vin}"
            )

        condition_items = sorted(response["Items"], key=itemgetter('updated'), reverse=True)

        condition_item = validate_condition(condition_items[0])

        for item in condition_item['condition']['damages']:
            damage = validate_damage(item)
            damages.append(damage)

        LOGGER.debug({"damages": damages})

        for item in condition_item['condition']['tires']:
            tire = validate_tire(item)
            tires.append(tire)

        LOGGER.debug({"tires": tires})

        return {
            "damages": damages,
            "tireInfo": tires
        }
    except MultipleInvalid as v_err:
        LOGGER.warn(
            {
                "type": "MultipleInvalid",
                "error": str(v_err),
                "event": event,
            }
        )

    except KeyError as k_error:
        LOGGER.warn(
            {
                "type": "KeyError",
                "error": str(k_error),
                "event": event,
            }
        )

    except DynamoItemNotFound as d_error:
        LOGGER.warn(
            {
                "type": "DynamoItemNotFound",
                "error": str(d_error),
                "event": event,
            }
        )

    except Exception as e_error:
        LOGGER.warn(
            {
                "type": "Exception",
                "error": str(e_error),
                "event": event,
            }
        )


def get_labors(event, _):
    LOGGER.debug({"event": event})
    labors = []

    try:
        request_params = validate_primary_key(event)
        pk = request_params['pk']

        key_condition_expression = Key("pk").eq(pk) & \
            Key("sk").begins_with("labor#")

        args = {
            "KeyConditionExpression": key_condition_expression,
        }

        response = query(args)

        if not response.get("Items"):
            raise DynamoItemNotFound(
                404,
                f"No labors found for pk:{pk}"
            )

        labors = response["Items"]

        LOGGER.debug({"labors": labors})

        return labors
    except MultipleInvalid as v_err:
        LOGGER.warn(
            {
                "type": "MultipleInvalid",
                "error": str(v_err),
                "event": event,
            }
        )

    except KeyError as k_error:
        LOGGER.warn(
            {
                "type": "KeyError",
                "error": str(k_error),
                "event": event,
            }
        )

    except DynamoItemNotFound as d_error:
        LOGGER.warn(
            {
                "type": "DynamoItemNotFound",
                "error": str(d_error),
                "event": event,
            }
        )

    except Exception as e_error:
        LOGGER.warn(
            {
                "type": "Exception",
                "error": str(e_error),
                "event": event,
            }
        )


def not_found(headers={}, body=None):
    return get_response("200", headers, body)


def get_workorder_damage(event, _):
    LOGGER.debug({"event": event}),
    items = []

    try:
        request_params = validate_workorder_damage(event)
        request_body = request_params.get("body", {})
        site_id = request_body['site_id']
        work_order_number = request_body['work_order_number']
        item_code = request_body['item_code']
        damage = request_body['damage']

        key_condition_expression = Key("site_id").eq(site_id) & Key("work_order_number").eq(work_order_number)

        filter_exp = Attr("item_code").eq(item_code) & Attr("damage").eq(damage)

        args = {
            "IndexName": "index_site_work_order_number",
            "KeyConditionExpression": key_condition_expression,
            "FilterExpression": filter_exp
        }

        response = query(args)

        if not response.get("Items"):
            raise DynamoItemNotFound(
                404,
                f"No damage found for work_order_number:{work_order_number}"
            )

        items = response.get("Items", {})
        LOGGER.debug({"damage": items})
        if not items:
            return not_found(body=s_json.dumps([]))
        if items[0].get("charge_l_status"):
            shop_category = items[0]["charge_l_status"]["shop_code"]
        elif items[0].get("charge_p_status"):
            shop_category = items[0]["charge_p_status"]["shop_code"]
        else:
            shop_category = ""

        body = s_json.dumps({
            "shop_category": shop_category
        })
        status_code = 200

    except MultipleInvalid as v_err:
        LOGGER.warn(
            {
                "type": "MultipleInvalid",
                "error": str(v_err),
                "event": event,
            }
        )
        body = json.dumps(get_error("Bad Request", v_err.msg))
        status_code = 400

    except DynamoItemNotFound as d_error:
        LOGGER.warn(
            {
                "type": "DynamoItemNotFound",
                "error": str(d_error),
                "event": event,
            }
        )
        body = None
        status_code = 200

    except Exception as e_error:
        LOGGER.warn(
            {
                "type": "Exception",
                "error": str(e_error),
                "event": event,
            }
        )
        body = json.dumps(get_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(e_error)))
        status_code = HTTPStatus.INTERNAL_SERVER_ERROR

    response = get_response(status_code, HEADERS, body)

    return response
