"""
    order approval functions
"""
# pylint: disable=too-many-locals, too-many-arguments
import copy
from decimal import Decimal

from botocore.exceptions import ClientError
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid

from damages import build_damages
from validation import valid_approval


def build_order(new_record, old_record):
    """
        build approval order dictionary
    """

    order = None
    approval_service = {"approval_id": new_record["approval_id"]}

    try:
        order = valid_approval(new_record["order"])
        order.pop("consignment")
        order.update({"damages": build_damages(order, new_record["work_order_key"])})
        order.update(approval_service)
        order.update({"initial_approval_date": order["createdOn"]})

        if material_change(new_record, old_record):
            order.update({"recent_approval_date": order["updatedOn"]})

    except MultipleInvalid as validation_error:
        message = {
            "message": "Ignoring approval event",
            "reason": str(validation_error),
            "event": new_record,
        }

        LOGGER.warning(message)

    return order


def get_order_approval(new_image, old_image):
    """
        return ordere approval column
    """

    column = None
    try:
        data = build_order(new_image, old_image)
        column = {
            "name": "order_approval_service",
            "data": data,
            "store_wo_record": store_wo_record,
        }
    except KeyError as k_error:
        message = {
            "message": "Bad approval event",
            "reason": str(k_error),
            "event": new_image,
        }
        LOGGER.warning(message)

    return column


def store_wo_record(new_image, updated, column, unit, table, new_column=False):
    """
        Store workorder service to dynamodb
    """
    work_order_key = new_image["sblu"]
    work_order_key += "#"
    work_order_key += new_image["site_id"]

    if isinstance(updated, float):
        updated = Decimal(str(updated))

    if isinstance(updated, str):
        updated = Decimal(updated)

    key = {"work_order_key": work_order_key, "site_id": new_image["site_id"]}
    damages = column["data"].get("damages", {})

    LOGGER.debug(
        {
            "work_order_key": work_order_key,
            "key": key,
            "column_name": column["name"],
            "column_data": column["data"],
            "damages": damages,
        }
    )

    update_expression = "set \
    #sblu = :sblu,\
    #vin = :vin,\
    #work_order_number = :work_order_number,\
    #manheim_account_number = :manheim_account_number,\
    #company_name = :company_name,\
    #group_code = :group_code,\
    #check_in_date = :check_in_date,\
    #damages = :damages,\
    #column_updated = :updated"

    condition_expression = (
        "attribute_not_exists(#column_updated) OR #column_updated < :updated"
    )

    expression_attribute_names = {
        "#sblu": "sblu",
        "#vin": "vin",
        "#work_order_number": "work_order_number",
        "#manheim_account_number": "manheim_account_number",
        "#company_name": "company_name",
        "#damages": "damages",
        "#group_code": "group_code",
        "#check_in_date": "check_in_date",
        "#column_updated": column.get("updated_name", column["name"] + "_updated"),
    }

    expression_attribute_values = {
        ":sblu": new_image["sblu"],
        ":vin": new_image["vin"],
        ":work_order_number": new_image["work_order_number"],
        ":manheim_account_number": new_image["consignment"]["manheimAccountNumber"],
        ":company_name": unit["contact"]["companyName"],
        ":group_code": unit["account"]["groupCode"],
        ":check_in_date": new_image["consignment"]["checkInDate"],
        ":damages": damages,
        ":updated": updated,
    }

    if column is not None and new_column:

        copy_data = copy.deepcopy(column["data"])
        for k, v in copy_data.items():
            if v == "Remove":
                column["data"].pop(k)

        update_expression += ", #column_name = :column_data"
        condition_expression = "(attribute_not_exists(#column_updated) OR \
            #column_updated < :updated) AND attribute_not_exists(#column_name)"

        expression_attribute_names.update({"#column_name": column["name"]})

        expression_attribute_values.update({":column_data": column["data"]})

    if column is not None and not new_column:
        update_expression += "," + ",".join(
            [
                "#rpp_"
                + column["name"]
                + ".#rpp_"
                + k
                + " = :"
                + column["name"]
                + "_"
                + k
                for k in column["data"].keys()
                if column["data"][k] != "Remove"
            ]
        )

        expression_attribute_names.update({"#rpp_" + column["name"]: column["name"]})

        expression_attribute_names.update(
            {"#rpp_" + k: k for k in column["data"].keys()}
        )

        expression_attribute_values.update(
            {
                ":" + column["name"] + "_" + k: column["data"][k]
                for k in column["data"].keys()
                if column["data"][k] != "Remove"
            }
        )

    remove_fields = [
        "#rpp_" + column["name"] + ".#rpp_" + k
        for k in column["data"].keys()
        if column["data"][k] == "Remove"
    ]

    LOGGER.info({"remove_fields": remove_fields})
    LOGGER.info({"column data": column["data"]})

    if remove_fields:
        update_expression += " REMOVE " + ",".join(remove_fields)

    LOGGER.info(
        {
            "message": "store record via the following",
            "update_expression": update_expression,
            "condition_expression": condition_expression,
            "expression_attribute_names": expression_attribute_names,
            "expression_attribute_values": expression_attribute_values,
        }
    )

    response = None
    try:
        response = table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ConditionExpression=condition_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW",
        )
    except ClientError as c_err:
        error_code = c_err.response["Error"]["Code"]
        if error_code == "ValidationException" and not new_column:
            column.get("store_wo_record", store_wo_record)(
                new_image, updated, column, unit, table, new_column=True
            )
        else:
            raise c_err

    return response


def material_change(new_image, old_image):
    if old_image:
        new_damages = set(extract_damage_info(new_image).items())
        old_damages = set(extract_damage_info(old_image).items())
        return new_damages.difference(old_damages)
    else:
        return True


def extract_damage_info(image):
    damages = {}
    for damage in image.get("order", {}).get("condition", {}).get("damages", {}):
        damages.update(
            {
                f'{damage["itemCode"]}#{damage["subItemCode"]}#{damage["damageCode"]}': damage.get(
                    "approved", False
                )
            }
        )
    return damages
