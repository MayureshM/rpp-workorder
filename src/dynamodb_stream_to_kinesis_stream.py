"""
pulling the records from dynamodb stream and pushing them to kinesis stream
"""
import time
import json
import sys
import uuid
import boto3
from aws_xray_sdk.core import xray_recorder  # noqa: F401
from aws_xray_sdk.core import patch_all
from botocore.exceptions import ClientError
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import Any

patch_all()

ENV = Env()
KINESIS = boto3.client("kinesis")
RECON_WORKORDER_KINESIS_STREAM_ARN = ENV("RECON_WORKORDER_KINESIS_STREAM_ARN", validate=Any(str))


def handler(event, context):
    LOGGER.info({"event": event})
    messages_to_reprocess = []
    batch_failure_response = {"batchItemFailures": messages_to_reprocess}

    LOGGER.info({"message": f"Got {len(event['Records'])} record(s) to process"})
    response = {}
    for dynamodb_stream_record in event["Records"]:
        try:
            add_additional_fields_to_record(dynamodb_stream_record)
            kwargs = {"Data": json.dumps(dynamodb_stream_record).encode("utf-8"),
                      "PartitionKey": get_partition_key(dynamodb_stream_record),
                      "StreamARN": RECON_WORKORDER_KINESIS_STREAM_ARN}
            LOGGER.info({"message": "Putting the data into the kinesis stream", "kwargs": kwargs})
            response = KINESIS.put_record(**kwargs)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                LOGGER.info({"message": "Successfully put the data into the kinesis stream", "response": response})
                continue

            LOGGER.critical({"message": "Error while putting the data into the kinesis stream"})
            messages_to_reprocess.append({"itemIdentifier": dynamodb_stream_record["dynamodb"]["SequenceNumber"]})
            message = {
                "message": f"Error while putting the data into the kinesis stream {RECON_WORKORDER_KINESIS_STREAM_ARN}. Will be returned for reprocessing",
                "reason": response,
                "dynamodb_stream_sequence_number": dynamodb_stream_record["dynamodb"]["SequenceNumber"],
                "chunk_size": sys.getsizeof({"Data": kwargs["Data"], "PartitionKey": kwargs["PartitionKey"]}),
                "chunk": dynamodb_stream_record,
            }
            LOGGER.critical(message)
            return batch_failure_response
        except ClientError as err:
            messages_to_reprocess.append({"itemIdentifier": dynamodb_stream_record["dynamodb"]["SequenceNumber"]})
            message = {
                "message": f"Error while putting the data into the kinesis stream {RECON_WORKORDER_KINESIS_STREAM_ARN}. Will be returned for reprocessing",
                "reason": str(err),
                "dynamodb_stream_sequence_number": dynamodb_stream_record["dynamodb"]["SequenceNumber"],
                "chunk_size": sys.getsizeof({"Data": kwargs["Data"], "PartitionKey": kwargs["PartitionKey"]}),
                "chunk": dynamodb_stream_record,
            }
            LOGGER.critical(message)
            return batch_failure_response

    return batch_failure_response


def add_additional_fields_to_record(record):
    """ add additional fields to the record """
    if "dynamodb" not in record:
        return record

    record["tableName"] = "rpp-recon-work-order"
    record["recordFormat"] = "application/json"
    record["userIdentity"] = None
    record["dynamodb"]["ApproximateCreationDateTime"] = int(round(time.time() * 1000))

    return record


def get_partition_key(record):
    """ get dynamodb stream partition key """
    try:
        partition_key = record["dynamodb"]["NewImage"]["pk"]["S"]
    except KeyError:
        try:
            partition_key = record["dynamodb"]["OldImage"]["pk"]["S"]
        except KeyError:
            partition_key = str(uuid.uuid4())

    return str(partition_key)
