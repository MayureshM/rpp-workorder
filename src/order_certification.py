"""
    order certification functions
"""
import copy
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError
from environs import Env
from voluptuous import MultipleInvalid
from voluptuous import Any
from rpp_lib.logs import LOGGER
from validation import (
    valid_certification_completed,
    valid_certification_updated,
    valid_certification_canceled
)

ENV = Env()
DYNAMO = boto3.resource('dynamodb')
CATEGORY_TABLE = DYNAMO.Table(name=ENV('CATEGORY_TABLE', validate=Any(str)))


def build_order(new_record):
    """
        build certification order dictionary
    """

    try:
        order = valid_certification_canceled(new_record["order"])
        order.update({"action": "delete"})

    except MultipleInvalid:

        order = None
        item = CATEGORY_TABLE.get_item(Key={"key": "CERT"})["Item"]
        cert_service = {
            "certification_id": new_record["certification_id"],
            "shop": item["category"],
            "shop_description": item["shopDescription"],
            "labor_hours": item["hours"],
            "labor_name": item["laborName"],
            "labor_status": "Ready for Repair",
            "product_id": item["productId"]
        }

        try:
            order = valid_certification_updated(new_record["order"])
            order.pop("consignment")
            order.update(cert_service)
            order.update({"request_date": order["createdOn"]})
        except MultipleInvalid:
            try:
                order = valid_certification_completed(new_record["order"])
                order.pop("consignment")
                order.update(cert_service)
                order.update({"labor_status": "Completed"})
                order.update({"request_date": order["createdOn"]})
                order.update({"complete_date": order["updatedOn"]})

            except MultipleInvalid as validation_error:
                message = {
                    "message": "Ignoring certification event",
                    "reason": str(validation_error),
                    "event": new_record
                }

                LOGGER.warning(message)

    return order


def get_order_certification(new_image):
    """
        return order certification column
    """

    column = None
    try:
        data = build_order(new_image)
        if data is not None:
            column = {
                "name": "cert_service",
                "store_wo_record": store_wo_record,
                "data": data
            }
    except KeyError as k_error:
        message = {
            "message": "Bad certification event",
            "reason": str(k_error),
            "event": new_image
        }
        LOGGER.warning(message)

    return column


# pylint: disable=too-many-arguments, too-many-locals
def store_wo_record(new_image, updated, column, unit, table, new_column=False):
    """
        Store workorder service to dynamodb or remove the cert_service attribute
    """
    work_order_key = new_image["sblu"]
    work_order_key += "#"
    work_order_key += new_image["site_id"]

    if isinstance(updated, float):
        updated = Decimal(str(updated))

    if isinstance(updated, str):
        updated = Decimal(updated)

    if column.get("data", {}).get("action", None) == "delete":
        response = table.update_item(
            Key={
                "work_order_key": work_order_key,
                "site_id": new_image['site_id'],
            },
            UpdateExpression="SET #column_updated = :updated REMOVE #cert_service",
            ExpressionAttributeNames={
                "#cert_service": "cert_service",
                "#column_updated": column.get("updated_name", column["name"] + "_updated")
            },
            ExpressionAttributeValues={
                ":updated": updated
            },
            ReturnValues="UPDATED_NEW"
        )
        return

    LOGGER.debug({"work_order_key": work_order_key})

    LOGGER.debug({column["name"]: column})

    key = {
        "work_order_key": work_order_key,
        "site_id": new_image["site_id"]
    }

    update_expression = "set \
    #sblu = :sblu,\
    #vin = :vin,\
    #work_order_number = :work_order_number,\
    #manheim_account_number = :manheim_account_number,\
    #company_name = :company_name,\
    #group_code = :group_code,\
    #check_in_date = :check_in_date,\
    #column_updated = :updated"

    condition_expression = "attribute_not_exists(#column_updated) OR \
    #column_updated < :updated"

    expression_attribute_names = {
        "#sblu": "sblu",
        "#vin": "vin",
        "#work_order_number": "work_order_number",
        "#manheim_account_number": "manheim_account_number",
        "#company_name": "company_name",
        "#group_code": "group_code",
        "#check_in_date": "check_in_date",
        "#column_updated": column.get(
            "updated_name", column["name"] + "_updated")
    }

    expression_attribute_values = {
        ":sblu": new_image["sblu"],
        ":vin": new_image["vin"],
        ":work_order_number": new_image["work_order_number"],
        ":manheim_account_number": new_image[
            "consignment"]["manheimAccountNumber"],
        ":company_name": unit["contact"]["companyName"],
        ":group_code": unit["account"]["groupCode"],
        ":check_in_date": new_image[
            "consignment"]["checkInDate"],
        ":updated": updated
    }

    if column is not None and new_column:

        copy_data = copy.deepcopy(column["data"])
        for k, v in copy_data.items():
            if v == "Remove":
                column["data"].pop(k)

        update_expression += ", #column_name = :column_data"
        condition_expression = "(attribute_not_exists(#column_updated) OR \
            #column_updated < :updated) AND \
            attribute_not_exists(#column_name)"

        expression_attribute_names.update(
            {"#column_name": column["name"]}
        )

        expression_attribute_values.update(
            {":column_data": column["data"]}
        )

    if column is not None and not new_column:
        update_expression += "," + ",".join([
            "#rpp_" + column[
                "name"] + ".#rpp_" + k + " = :" + column[
                    "name"] + "_" + k for k in column[
                        "data"].keys() if column["data"][k] != "Remove"])

        expression_attribute_names.update(
            {"#rpp_" + column["name"]: column["name"]}
        )

        expression_attribute_names.update(
            {"#rpp_" + k: k for k in column["data"].keys()}
        )

        expression_attribute_values.update(
            {":" + column["name"] + "_" + k: column[
                "data"][k] for k in column[
                    "data"].keys() if column["data"][k] != "Remove"}
        )

    remove_fields = [
        "#rpp_" + column[
            "name"] + ".#rpp_" + k for k in column[
                "data"].keys() if column["data"][k] == "Remove"]

    LOGGER.info({"remove_fields": remove_fields})
    LOGGER.info({"column data": column["data"]})

    if remove_fields:
        update_expression += " REMOVE " + ",".join(remove_fields)

    LOGGER.info(
        {
            'message': "store record via the following",
            'update_expression': update_expression,
            'condition_expression': condition_expression,
            'expression_attribute_names': expression_attribute_names,
            'expression_attribute_values': expression_attribute_values
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
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as c_err:
        error_code = c_err.response["Error"]["Code"]
        if error_code == 'ValidationException':
            column.get(
                'store_wo_record',
                store_wo_record
            )(new_image, updated, column, unit, table, new_column=True)
        else:
            raise c_err

    return response
