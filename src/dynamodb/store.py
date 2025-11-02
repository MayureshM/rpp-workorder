"""
    dynamodb file to store/retrieve work-order information from rpp-recon-work-order
"""
import boto3
from aws_xray_sdk.core import patch_all
from boto3.dynamodb.conditions import Key, ConditionBase
from environs import Env
from rpp_lib.dynamodb import get_item
from rpp_lib.logs import LOGGER
from voluptuous import Any
from utils.common import sanitize_for_logging

patch_all()

ENV = Env()
DYNAMO = boto3.resource("dynamodb")
WORK_ORDER_TABLE_NAME = ENV("WORKORDER_AM_TABLE", validate=Any(str))
WO_TABLE = DYNAMO.Table(WORK_ORDER_TABLE_NAME)


def get_work_order(pk: str, sk: str) -> dict:
    """
    return record info for a given workorder
    """
    LOGGER.debug({"pk": pk, "sk": sk})
    work_order_record = {"pk": pk, "sk": sk}

    response = get_item(WORK_ORDER_TABLE_NAME, work_order_record)

    if not response.get("Item"):
        raise DynamoItemNotFound(404, f"work_order info for record: {pk}")

    item = response["Item"]
    return item


def get_work_oder_index(key: dict, index: str) -> list:
    """
    return list of records for a given index
    """
    LOGGER.debug({"key": key, "index": index})

    key_items = list(key.items())
    key_ce = Key(key_items[0][0]).eq(key_items[0][1])
    if len(key_items) > 1:
        key_ce = key_ce & Key(key_items[1][0]).eq(key_items[1][1])

    response = WO_TABLE.query(
        IndexName=index,
        KeyConditionExpression=key_ce,
    )

    if not response.get("Items"):
        raise DynamoItemNotFound(404, f"No records found for key:{key} index:{index}")

    items = response["Items"]
    return items


def get_all_by_pk(pk: str, filter_expression: str = None) -> list:
    """
    return records for a given pk or filter expression
    """
    response = None
    if pk is not None:
        key_condition_exp = Key("pk").eq(pk)
        if filter_expression:
            response = WO_TABLE.query(
                KeyConditionExpression=key_condition_exp,
                FilterExpression=filter_expression,
            )
        else:
            response = WO_TABLE.query(KeyConditionExpression=key_condition_exp)
    else:
        raise ValueError("Parameters missing or invalid")

    items = response.get("Items", [])
    if not items:
        raise DynamoItemNotFound(404, f"work_order info for record: {pk}")

    return items


def update_document_for_pk_and_sk(pk: str, sk: str, document: dict, condition_expression: str = None,
                                  condition_obj: ConditionBase = None) -> dict:
    """
    Update work-order row info for a given workorder with optional conditional expression

    Args:
        pk: Partition key value
        sk: Sort key value
        document: Dictionary containing attributes to update
        condition_expression: String condition expression (optional)
        condition_obj: ConditionBase object for complex conditions (optional)

    Returns:
        dict: Updated attributes

    Raises:
        ValueError: If required parameters are missing or invalid
        ConditionalCheckFailedException: If the condition expression fails
        ClientError: For other DynamoDB errors
    """
    from botocore.exceptions import ClientError

    if not pk or not sk:
        raise ValueError("Primary key (pk) and sort key (sk) are required")

    if not document:
        raise ValueError("Document to update cannot be empty")

    key = {"pk": pk, "sk": sk}

    LOGGER.debug({"key to update": key})

    # Sanitize document keys to prevent NoSQL injection
    sanitized_document = {}
    for k, v in document.items():
        # Ensure key is a valid attribute name (alphanumeric and underscore only)
        if isinstance(k, str) and (k.replace('_', '').isalnum()):
            sanitized_document[k] = v
        else:
            LOGGER.warning(f"Skipping invalid attribute name: {k}")

    if not sanitized_document:
        raise ValueError("No valid attributes to update")

    attribute_names = {}
    attribute_values = {}

    update_expression = "set "
    update_expression += ",".join(["#" + k + " = :" + k for k in sanitized_document.keys()])

    attribute_names.update({"#" + k: k for k in sanitized_document.keys()})
    attribute_values.update({":" + k: sanitized_document[k] for k in sanitized_document.keys()})

    # Prepare update parameters
    update_params = {
        "Key": key,
        "UpdateExpression": update_expression,
        "ExpressionAttributeNames": attribute_names,
        "ExpressionAttributeValues": attribute_values,
        "ReturnValues": "UPDATED_NEW",
    }

    # Add conditional expression if provided
    if condition_obj:
        update_params["ConditionExpression"] = condition_obj
    elif condition_expression:
        update_params["ConditionExpression"] = condition_expression
    data_to_log = sanitize_for_logging({
        "key": key,
        "update_expression": update_expression,
        "condition_expression": update_params.get("ConditionExpression"),
        "attribute_names": attribute_names,
        "attribute_values": attribute_values,
    })
    LOGGER.info(
        data_to_log
    )

    try:
        response = WO_TABLE.update_item(**update_params)
        return response["Attributes"]
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ConditionalCheckFailedException':
            data_to_log = sanitize_for_logging({
                "message": "Conditional update failed",
                "key": key,
                "condition": update_params.get("ConditionExpression"),
                "error": str(e)
            })
            LOGGER.warning(
                data_to_log
            )
            # Re-raise the specific exception for the caller to handle
            raise
        else:
            data_to_log = sanitize_for_logging({
                "message": "DynamoDB update failed",
                "key": key,
                "error_code": error_code,
                "error": str(e)
            })
            LOGGER.error(
                data_to_log
            )
            raise


def put_work_order(workorder: str,
                   sk: str,
                   record: dict,
                   condition: str = None,
                   remove_attributes: str = None,
                   update_attribute="updated",
                   condition_obj: ConditionBase = None
                   ) -> dict:
    """
    Update work-order row info for a given workorder
    """

    if remove_attributes is None:
        remove_attributes = []
    LOGGER.debug({"workorder": workorder, "sk": sk})
    key = {"pk": f"workorder:{workorder}", "sk": sk}

    attribute_names = {}
    attribute_values = {}

    update_expression = "set "
    update_expression += ",".join(["#" + k + " = :" + k for k in record.keys()])

    if remove_attributes:
        update_expression += " remove "
        update_expression += ", ".join(remove_attributes)

    attribute_names.update({"#" + k: k for k in record.keys()})

    attribute_values.update({":" + k: record[k] for k in record.keys()})
    if condition_obj:
        condition_expression = condition_obj
    else:
        condition_expression = f"attribute_not_exists(#{update_attribute}) OR #{update_attribute} <= :{update_attribute}"
        if condition:
            condition_expression += " " + condition

    LOGGER.info(
        {
            "key": key,
            "update_expression": update_expression,
            "condition_expression": condition_expression,
            "attribute_names": attribute_names,
            "attribute_values": attribute_values,
        }
    )

    response = WO_TABLE.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ConditionExpression=condition_expression,
        ExpressionAttributeNames=attribute_names,
        ExpressionAttributeValues=attribute_values,
        ReturnValues="UPDATED_NEW",
    )

    return response["Attributes"]


class DynamoItemNotFound(Exception):
    pass


def delete_record(workorder: str, sk: str):
    """
    Delete work-order row info for a given workorder
    """

    key = {"pk": f"workorder:{workorder}", "sk": sk}

    LOGGER.debug({"key to delete": key})

    WO_TABLE.delete_item(
        Key=key,
        ConditionExpression="attribute_exists(pk)",
    )


def query(kwargs) -> dict:
    return WO_TABLE.query(**kwargs)
