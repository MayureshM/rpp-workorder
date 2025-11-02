"""
Module to manage local vs non local dynamodb calls
"""

import boto3
import os
import time

from boto3.dynamodb.conditions import Key
from rpp_lib.logs import LOGGER

HEADERS = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}


def get_resource():
    """
    high-level service class recommended to be used by boto
    """
    return (
        boto3.resource("dynamodb", endpoint_url="http://local-dynamodb:8000")
        if os.getenv("AWS_SAM_LOCAL")
        else boto3.resource("dynamodb")
    )


def get_client():
    """
    low-level class object in which you must specify the targeting resource for every call
    """
    return (
        boto3.client("dynamodb", endpoint_url="http://local-dynamodb:8000")
        if os.getenv("AWS_SAM_LOCAL")
        else boto3.client("dynamodb")
    )


def query_table(table_name, filter_key=None, filter_value=None):
    """
    Perform a query operation on the table. Can specify filter_key
    (col name) and its value to be filtered. Returns the response.
    """
    table = get_resource().Table(table_name)

    if filter_key and filter_value:
        filtering_exp = Key(filter_key).eq(filter_value)
        response = table.query(KeyConditionExpression=filtering_exp)
    else:
        response = scan_allpages(table_name)

    return response


def get_metadata(table_name):
    """
    Get some metadata about chosen table.
    """
    table = get_resource().Table(table_name)

    return {
        "num_items": table.item_count,
        "primary_key_name": table.key_schema[0],
        "status": table.table_status,
        "bytes_size": table.table_size_bytes,
        "global_secondary_indices": table.global_secondary_indexes,
    }


def get_item(table_name, pk_name, pk_value):
    """
    Return item by primary key.
    """
    table = get_resource().Table(table_name)
    response = table.get_item(Key={pk_name: pk_value})

    return response


def get_item_by_key(table_name, key):
    """
    Return item by primary key.
    """
    table = get_resource().Table(table_name)
    response = table.get_item(Key=key)

    return response


def create_item(table_name, col_dict):
    """
    Add one item (row) to table. col_dict is a dictionary {col_name: value}.
    If the item has the same primary key as an item that already exists
    in the table, then the item in the table is completely replaced.
    """
    table = get_resource().Table(table_name)
    response = table.put_item(Item=col_dict)

    return response


def remove_item(table_name, key):
    """
    Delete an item (row) in table by its primary key.
    """
    table = get_resource().Table(table_name)
    response = table.delete_item(Key=key)

    return response


def scan_firstpage(table_name, filter_key=None, filter_value=None):
    """
    Perform a scan operation on table. Can specify filter_key (col name)
    and its value to be filtered. This gets only first page of results in
    pagination. Returns the response.
    """
    table = get_resource().Table(table_name)

    if filter_key and filter_value:
        filtering_exp = Key(filter_key).eq(filter_value)
        response = table.scan(FilterExpression=filtering_exp)
    else:
        response = table.scan()

    return response


def scan_allpages(table_name, filter_key=None, filter_value=None):
    """
    Perform a scan operation on table. Can specify filter_key (col name)
    and its value to be filtered. This gets all pages of results.
    Returns list of items.
    """
    table = get_resource().Table(table_name)

    if filter_key and filter_value:
        filtering_exp = Key(filter_key).eq(filter_value)
        response = table.scan(FilterExpression=filtering_exp)
    else:
        response = table.scan()

    items = response["Items"]
    while True:
        if response.get("LastEvaluatedKey"):
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items += response["Items"]
        else:
            break
    return items


def query(
    table_name,
    p_key=None,
    p_value=None,
    s_key=None,
    s_value=None,
    attributes=None,
    secondary_index=None,
):
    """
    Perform a query operation on the table.
    -   table_name: Name of the table
    -   p_key: Primary key
    -   p_value: Primary key value
    -   s_key: Key used alogn with the primary key to sort the items (Optional)
    -   s_key: Value of the sort key (Optional)
    -   atributes: values separeted with commas
        (If not specified, all attributes will be returned)
    Returns the response.
    """
    table = get_resource().Table(table_name)
    filter_expression = Key(p_key).eq(p_value)
    if s_value is not None and s_key is not None:
        filter_expression = Key(p_key).eq(p_value) & Key(s_key).eq(s_value)
    if secondary_index is None:
        if attributes is None:
            response = table.query(KeyConditionExpression=filter_expression)
        else:
            response = table.query(
                KeyConditionExpression=filter_expression,
                ProjectionExpression=attributes,
            )
    else:
        if attributes is None:
            response = table.query(
                IndexName=secondary_index, KeyConditionExpression=filter_expression
            )
        else:
            response = table.query(
                IndexName=secondary_index,
                KeyConditionExpression=filter_expression,
                ProjectionExpression=attributes,
            )
    return response


def update(table_name, key, update_dict):
    """
    Perform an UPDATE operation on table. Can specify key to find the record
    and a dictionary is passed to update feilds of the record (new fields can be stored).
    """
    table = get_resource().Table(table_name)
    update_expression = "set "
    update_expression += ",".join(
        ["#" + k + " = :" + k for k in update_dict.keys() if update_dict[k] is not None]
    )

    attribute_names = {
        "#" + k: k for k in update_dict.keys() if update_dict[k] is not None
    }

    attribute_values = {
        ":" + k: update_dict[k]
        for k in update_dict.keys()
        if update_dict[k] is not None
    }

    condition_expression = "attribute_not_exists(#updated) OR #updated <= :updated"

    response = table.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ConditionExpression=condition_expression,
        ExpressionAttributeNames=attribute_names,
        ExpressionAttributeValues=attribute_values,
        ReturnValues="UPDATED_NEW",
    )
    return response


def delete_field_item(table_name, key, attribute):
    """
    Perform an UPDATE operation on a table. Can specify key to find the record
    and an attribute is passed to update fields of the record (it will the attribute).
    """
    table = get_resource().Table(table_name)
    expression = "remove #delete_attribute"
    response = table.update_item(
        Key=key,
        UpdateExpression=expression,
        ExpressionAttributeNames={"#delete_attribute": str(attribute)},
        ReturnValues="UPDATED_NEW",
    )
    return response


def get_response(status_code, headers, body):
    """build success response"""

    headers.update({"Access-Control-Allow-Origin": "*"})
    headers.update({"Access-Control-Allow-Headers": "*"})
    headers.update({"Access-Control-Allow-Methods": "OPTIONS, POST, PUT, DELETE, GET"})

    response = {"statusCode": status_code, "headers": headers, "body": body}

    LOGGER.debug({"response": response})

    return response


def get_error(code, message):
    """build error response"""

    return {"errors": [{"code": code, "message": message}]}


def convert_to_date_stamp(timestamp):

    if type(timestamp) is not int:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(timestamp)))
    else:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp))
