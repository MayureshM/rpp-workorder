"""
    labor status events functions
"""
import boto3
from botocore.exceptions import ClientError

# pylint: disable=too-many-locals, too-many-arguments
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid

from validation import valid_labor_condition_status, valid_labor_fee_status

ENV = Env()
DYNAMO = boto3.resource("dynamodb")


def build_order(new_record):
    """
        build labor status order dictionary
    """
    LOGGER.debug("Getting labor-status to build order")

    order = {}

    try:
        labor_fee = valid_labor_fee_status(new_record)
        order.update({"event": "FEE"})
        order.update({k: v for k, v in labor_fee["current_status"].items()})
    except MultipleInvalid as validation_error:
        message = {
            "message": "Not labor fee, trying labor condition",
            "reason": str(validation_error),
            "event": new_record,
        }

        LOGGER.warning(message)
        try:
            labor = valid_labor_condition_status(new_record)
            order.update({"event": "CONDITION"})
            order.update({"current_status": labor["current_status"]})
            if labor.get("charge_l_status"):
                order.update({"charge_l_status": labor["charge_l_status"]})
            if labor.get("charge_p_status"):
                order.update({"charge_p_status": labor["charge_p_status"]})

        except MultipleInvalid as validation_error:
            message = {
                "message": "Ignoring labor status event, no valid labor",
                "reason": str(validation_error),
                "event": new_record,
            }

            LOGGER.warning(message)

    return order


def get_labor_status(new_image):
    """
        return labor status column
    """

    column = None
    try:
        data = build_order(new_image)
        if data is not None:

            name = new_image["sk"]
            labor_type = data.get("event")
            if labor_type == "FEE":
                name = name.lower()
                if name == "cert":
                    name = "cert_service"

            column = {
                "ad_hoc_damage": new_image.get("ad_hoc_damage", False),
                "data": data,
                "labor_type": labor_type,
                "name": name,
                "store_wo_record": store_wo_record,
            }

    except KeyError as k_error:
        message = {
            "message": "Bad labor status event, faile to get labor",
            "reason": str(k_error),
            "event": new_image,
        }
        LOGGER.warning(message)

    return column


def build_expressions(column, updated):
    """
        Build expressions for the store wo record function
    """

    if column["labor_type"] == "FEE":
        condition_expression = (
            "(attribute_exists(#column_name) AND #column_updated <= :updated)"
        )
        if column["name"] == "ucfin":
            condition_expression = (
                "attribute_exists(#column_name) AND "
                "((attribute_exists(#column_updated) AND attribute_exists(#column_updated_ucfin) AND "
                "#column_updated <= :updated AND #column_updated_ucfin <= :updated) OR "
                "(attribute_exists(#column_updated) AND #column_updated <= :updated) OR "
                "(attribute_exists(#column_updated_ucfin) AND #column_updated_ucfin <= :updated))"
            )

        update_expression = "SET #column_updated = :updated," + ", ".join(
            [
                "#column_name" + ".#" + k + " = :" + "rpp_" + k
                for k, v in column["data"].items()
                if v != "Remove" and k != "labor_type" and k != column["name"]
            ]
        )

        if any(value == "Remove" for value in column["data"].values()):
            update_expression += " REMOVE " + ", ".join(
                [
                    "#column_name" + ".#" + k
                    for k, v in column["data"].items()
                    if v == "Remove"
                ]
            )

        expression_attribute_names = {
            "#column_name": column["name"],
            "#column_updated": column["name"] + "_updated",
        }
        if column["name"] == "ucfin":
            expression_attribute_names.update(
                {"#column_updated_ucfin": "ucfin_vcf_event"}
            )
    else:
        if column["ad_hoc_damage"]:
            column_name = "ad_hoc_damages"
        else:
            column_name = "damages"

        condition_expression = (
            f"attribute_exists({column_name}.#damage_id) OR "
            f"{column_name}.#damage_id.updated <=:updated"
        )

        update_expression = (
            f"SET {column_name}.#damage_id.updated = :updated, "
            + ", ".join(
                [
                    f"{column_name}.#damage_id.#{k} = :rpp_{k}"
                    for k, v in column["data"].items()
                    if v != "Remove" and k != "labor_type" and k != column["name"]
                ]
            )
        )

        if any(value == "Remove" for value in column["data"].values()):
            update_expression += " REMOVE " + ", ".join(
                [
                    f"{column_name}.#damage_id.#{k}"
                    for k, v in column["data"].items()
                    if v == "Remove"
                ]
            )

        expression_attribute_names = {"#damage_id": column["name"]}

    expression_attribute_values = {":updated": updated}

    for k, v in column["data"].items():
        if k != "labor_type":
            expression_attribute_names.update({"#" + k: k})
            if v != "Remove":
                expression_attribute_values.update({":rpp_" + k: v})

    return {
        "ConditionExpression": condition_expression,
        "ExpressionAttributeNames": expression_attribute_names,
        "ExpressionAttributeValues": expression_attribute_values,
        "UpdateExpression": update_expression,
    }


def store_wo_record(new_image, updated, column, unit, table, new_column=False):
    """
        Store workorder service to dynamodb
    """

    key = {
        "work_order_key": new_image["pk"],
        "site_id": new_image["site_id"],
    }

    params = build_expressions(column, updated)

    params.update({"Key": key, "ReturnValues": "UPDATED_NEW"})

    LOGGER.debug({"message": "store record via the following", "parameters": params})

    response = None
    try:
        response = table.update_item(**params)

    except ClientError as c_err:
        error_code = c_err.response["Error"]["Code"]
        if error_code != "ValidationException":
            raise c_err

    return response
