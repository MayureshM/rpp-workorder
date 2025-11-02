"""
    work credit events functions
"""

from botocore.exceptions import ClientError
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid

from validation import valid_work_credit_condition, valid_work_credit_fee

update_fields = ["sublet_vendor_id", "sublet_vendor_name", "work_credit"]


def build_work_credit(new_record):
    """
        build work_credit dictionary
    """

    work_credit = None

    try:
        if new_record.get("serial_id"):
            fee = valid_work_credit_fee(new_record)
            work_credit = {k: v for k, v in fee.items() if k in update_fields}

        elif new_record.get("item_code"):
            condition = valid_work_credit_condition(new_record)
            work_credit = {k: v for k, v in condition.items() if k in update_fields}

    except MultipleInvalid as validation_error:
        message = {
            "message": "Not a valid work credit event",
            "error": str(validation_error),
            "event": new_record,
        }

        LOGGER.error(message)

    return work_credit


def get_work_credit(new_image):
    """
        return work_credit data to update
    """

    column = None
    try:
        data = build_work_credit(new_image)
        if data is not None:
            if new_image.get("serial_id"):
                column = {
                    "data": data,
                    "fee": True,
                    "name": new_image["labor"].lower(),
                    "store_wo_record": store_wo_record,
                }

                if column["name"] == "cert":
                    column["name"] = "cert_service"

            elif new_image.get("item_code"):
                column = {
                    "data": data,
                    "fee": False,
                    "name": new_image["labor"],
                    "store_wo_record": store_wo_record,
                }

    except KeyError as k_error:
        message = {
            "message": "Bad work credit event",
            "reason": str(k_error),
            "event": new_image,
        }
        LOGGER.warning(message)

    return column


def build_expressions(column, updated):
    """
        Build expressions for the store wo record function
    """

    if column["fee"]:
        condition_expression = (
            "(attribute_exists(#column_name) AND #column_updated <= :updated)"
        )
        if column["name"] == "ucfin":
            condition_expression = "attribute_exists(#column_name) AND ((attribute_exists(#column_updated) AND "
            condition_expression += "attribute_exists(#column_updated_ucfin) AND #column_updated <= :updated AND "
            condition_expression += "#column_updated_ucfin <= :updated) OR "
            condition_expression += "(attribute_exists(#column_updated) AND #column_updated <= :updated) OR "
            condition_expression += "(attribute_exists(#column_updated_ucfin) AND #column_updated_ucfin <= :updated))"

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
        condition_expression = "attribute_exists(damages.#column_name) AND damages.#column_name.updated <= :updated"

        update_expression = "SET damages.#column_name.updated = :updated," + ", ".join(
            [
                "damages.#column_name" + ".#" + k + " = :" + "rpp_" + k
                for k, v in column["data"].items()
                if v != "Remove" and k != "labor_type" and k != column["name"]
            ]
        )

        if any(value == "Remove" for value in column["data"].values()):
            update_expression += " REMOVE " + ", ".join(
                [
                    "damages.#column_name" + ".#" + k
                    for k, v in column["data"].items()
                    if v == "Remove"
                ]
            )

        expression_attribute_names = {"#column_name": column["name"]}

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
        "work_order_key": new_image["work_order_key"],
        "site_id": new_image["site_id"],
    }

    params = build_expressions(column, updated)

    params.update({"Key": key, "ReturnValues": "UPDATED_NEW"})

    LOGGER.debug({"message": "store record via the following", "parameters": params})

    try:
        response = table.update_item(**params)

    except ClientError as c_err:
        error_code = c_err.response["Error"]["Code"]
        if error_code != "ValidationException":
            raise c_err

    return response
