import base64
import json
import logging as LOGGER
from decimal import Decimal
import base64
import time

import boto3
from boto3.dynamodb.conditions import Key
from faker import Faker
from rpp_python_testing_sdk.events.aws.dynamodb import DynamoDBEvent
from rpp_python_testing_sdk.events.aws.kinesis import KinesisStreamEvent
import time
from dynamodb_json import json_util as d_json

LAMBDA_CLIENT = boto3.client("lambda", region_name="us-east-1")


def call_lambda(fn_name, data, is_response_needed=False, body=False):
    LOGGER.debug("*** call_lambda begin ***")

    response = None

    lambda_response = LAMBDA_CLIENT.invoke(
        FunctionName=fn_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(data),
    )

    if not is_response_needed:
        return response
    if lambda_response:
        response = lambda_response["Payload"].read().decode("utf-8")
        response = json.loads(response)
        if body:
            response = json.dumps(response["body"])

    LOGGER.debug("*** call_lambda response ***")
    LOGGER.debug(response)
    return response


def cleanup_by_pk(file, tbl):
    LOGGER.debug("*** tear down ***")

    with open(file) as json_file:
        record = json.load(json_file)
    json_file.close()

    dynamodb_response = None

    for item in record:
        item = json.loads(json.dumps(item), parse_float=Decimal)
        pk = item["pk"]

        response = tbl.query(KeyConditionExpression=Key("pk").eq(pk))
        for record in response["Items"]:
            sk = record["sk"]
            dynamodb_response = tbl.delete_item(Key={"pk": pk, "sk": sk})

    LOGGER.debug("*** dynamodb_response ***")
    LOGGER.debug(dynamodb_response)


def cleanup(file, sk, tbl):
    LOGGER.debug("*** tear down ***")

    with open(file) as json_file:
        record = json.load(json_file)
    json_file.close()

    dynamodb_response = None

    for item in record["Records"]:
        item = json.loads(json.dumps(item), parse_float=Decimal)
        pk = item["pk"]

        response = tbl.query(
            KeyConditionExpression=Key("pk").eq(pk) & Key("sk").begins_with(sk)
        )
        for record in response["Items"]:
            sk = record["sk"]
            dynamodb_response = tbl.delete_item(Key={"pk": pk, "sk": sk})

    LOGGER.debug("*** dynamodb_response ***")
    LOGGER.debug(dynamodb_response)


def add_dynamodb_data(file, tbl):
    print("*** add_dynamodb_data ***")
    # add data to the dynamodb table
    LOGGER.debug("*** add_dynamodb_data begin ***")
    with open(file) as json_file:
        record = json.load(json_file)
    json_file.close()

    LOGGER.debug(record)

    dynamodb_response = None
    for item in record:
        _item = json.loads(json.dumps(item), parse_float=Decimal)
        dynamodb_response = tbl.put_item(Item=_item)

    LOGGER.debug("*** dynamodb_response ***")
    LOGGER.debug(dynamodb_response)


def create_stream_event(request_json, pk=None, sk=None):
    # If pk/sk not provided, try to get from request_json
    if pk is None:
        pk = request_json.get("pk")
    if sk is None:
        sk = request_json.get("sk")

    if not pk or not sk:
        raise ValueError("Primary key (pk) and sort key (sk) are required")

    event_name = "INSERT"
    new_image = request_json.pop("NewImage", None)
    old_image = request_json.pop("OldImage", None)
    if old_image and not new_image:
        event_name = "REMOVE"
    if new_image and old_image:
        event_name = "MODIFY"
    record = DynamoDBEvent(eventName=event_name, awsRegion="us-east-1")
    record["dynamodb"]["Keys"] = {"pk": {"S": pk}, "sk": {"S": sk}}

    if old_image:
        record["dynamodb"]["OldImage"] = old_image

    if new_image:
        record["dynamodb"]["NewImage"] = new_image

    else:
        request_data = request_json.copy()
        request_data.pop("OldImage", None)
        record["dynamodb"]["NewImage"] = request_data

    encoded = json.dumps(record).encode()

    event = KinesisStreamEvent(
        data=base64.b64encode(encoded).decode(),
        eventName=event_name,
        awsRegion="us-east-1",
    )

    random_dt = Faker().date_time_between(start_date="-30d", end_date="now")
    arrival_timestamp = (
        time.mktime(random_dt.timetuple()) + random_dt.microsecond / 1_000_000
    )
    event["Records"][0]["kinesis"]["approximateArrivalTimestamp"] = arrival_timestamp

    return event


def get_dynamodb_record_begins_with_sk(pk, sk, tbl):
    """Get DynamoDB records where sk begins with the specified value"""
    response = tbl.query(
        KeyConditionExpression=Key("pk").eq(pk) & Key("sk").begins_with(sk)
    )
    return response.get("Items", [])

  
def assert_equal(expected: dict, record: dict, ignore_fields=[]):
    fields_to_ignore = [
        "updated",
        "hr_updated",
    ]
    fields_to_ignore.extend(ignore_fields)
    cleaned_expected = {k: v for k, v in expected.items() if k not in fields_to_ignore}
    cleaned_record = {k: v for k, v in record.items() if k not in fields_to_ignore}
    assert cleaned_expected == cleaned_record


def get_json_content(file):
    with open(file) as json_file:
        record = json.load(json_file, parse_float=Decimal, parse_int=Decimal)
    json_file.close()
    return record

def dynamo_delete_records(pk, tbl):
    response = tbl.query(
        KeyConditionExpression=Key("pk").eq(pk),
    )
    items = response.get("Items", [])

    for item in items:
        tbl.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})


def create_dynamo_kinesis_event(event_name: str, record: dict, modified_data=None, removed_data=None):
    """This function takes dynamoDB json and create kinesis event"""
    db_keys = {"pk": record["pk"], "sk": record["sk"]}
    dynamo_json = DynamoDBEvent(
        eventName=event_name,
        dynamodb={
            "ApproximateCreationDateTime": int(time.time() * 1000),
            "Keys": d_json.dumps(db_keys, as_dict=True),
            "SequenceNumber": "12345689",
            "SizeBytes": 256,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    )

    if event_name == "INSERT":
        dynamo_json["dynamodb"]["NewImage"] = d_json.dumps(record, as_dict=True)
    elif event_name == "MODIFY":
        dynamo_json["dynamodb"]["OldImage"] = d_json.dumps(record, as_dict=True)
        new_item = record.copy()

        for key, value in modified_data.items():
            new_item[key] = value

        for key in removed_data:
            new_item.pop(key, None)
        dynamo_json["dynamodb"]["NewImage"] = d_json.dumps(new_item, as_dict=True)
    elif event_name == "REMOVE":
        dynamo_json["dynamodb"]["OldImage"] = d_json.dumps(record, as_dict=True)

    # Encode the dynamo json and produce kinesis event
    encoded_data = base64.b64encode(json.dumps(dynamo_json).encode()).decode()
    return KinesisStreamEvent(data=encoded_data)
