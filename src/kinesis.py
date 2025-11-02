"""
processor for approval stream
"""
import json

# import zlib
import sys
import time
import uuid
from decimal import Decimal

import boto3

# pylint: disable=unused-import
from aws_xray_sdk.core import xray_recorder  # noqa: F401
from aws_xray_sdk.core import patch_all
from botocore.exceptions import ClientError
from dynamodb_json import json_util
from environs import Env
from rpp_lib.logs import LOGGER
from rpp_lib.rpc import get_offering
from rpp_lib.validation import validate_unit
from voluptuous import Any

from event_stream import lookup_unit
from order_offering import get_order_offering
from validation import valid_new_image

patch_all()

ENV = Env()
DYNAMO = boto3.resource("dynamodb")
TABLE = DYNAMO.Table(ENV("WORKORDER_TABLE"))
KINESIS = boto3.client("kinesis")
STREAM = ENV("STREAM", validate=Any(str))
CHUNK_SIZE = int(ENV("CHUNK_SIZE", validate=Any(str)))
RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"


def get_partition_key(record):
    """ get stream partition key """
    try:
        partition_key = record["dynamodb"]["NewImage"]["work_order_key"]["S"]
    except KeyError:
        try:
            partition_key = record["dynamodb"]["OldImage"]["work_order_key"]["S"]
        except KeyError:
            partition_key = str(uuid.uuid4())

    return partition_key


def prep_data(record):
    """ trim and compress data """

    t_compress = time.monotonic()
    data = json.dumps(record).encode("utf-8")
    t_compress = time.monotonic() - t_compress

    LOGGER.debug(
        {
            "message": "Data trimmed and compressed",
            "record": record,
            "size of origional record ": sys.getsizeof(
                json.dumps(record).encode("utf-8")
            ),
            "size of record compressed": sys.getsizeof(data),
            "compression time": t_compress,
        }
    )

    return data


def get_attribute(record, path):
    """Get value from path inside record"""
    splitted_path = path.split(".")
    for index, new_path in enumerate(splitted_path):
        record = record.get(new_path, {} if index != len(splitted_path) - 1 else None)

    return record


def check_offering(record, work_order_key):
    """Verify if record has an existing un processed offering event"""
    LOGGER.debug(record)
    offering = {}

    offering = get_offering(None, work_order_key)
    LOGGER.debug(offering)

    if offering:
        try:
            max_record = max(
                offering, key=lambda x: get_attribute(x, "order.updatedOn")
            )

            new_image = json_util.loads(
                json.dumps(max_record, default=cast_to_int), parse_float=Decimal
            )

            LOGGER.debug({"new_image": new_image})

            column = get_order_offering(new_image)
            LOGGER.debug({"offering column": column})

            response = lookup_unit(new_image)
            unit = validate_unit(response)
            new_record = valid_new_image(new_image)
            column.get("store_wo_record")(
                new_record, new_record.get("updated"), column, unit, TABLE, True
            )
        except ClientError as c_error:
            LOGGER.warning(
                {"reason": str(c_error), "Error": c_error, "record": max_record}
            )
        except Exception as exc:
            LOGGER.error({"reason": str(exc), "exception": exc, "record": max_record})


def process_stream(event, _):
    """ handle dynamodb stream events """

    event_records = event["Records"]
    records = []

    for record in event_records:
        if record["eventName"] == "INSERT":
            work_order_key = get_partition_key(record)
            check_offering(record, work_order_key)

    records = [
        {"Data": prep_data(record), "PartitionKey": get_partition_key(record)}
        for record in event_records
    ]

    chunks = [
        records[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]  # noqa E203
        for i in range((len(records) + CHUNK_SIZE - 1) // CHUNK_SIZE)
    ]

    for chunk in chunks:
        try:
            response = KINESIS.put_records(StreamName=STREAM, Records=chunk)

            while response["FailedRecordCount"] > 0:
                failed_records = get_failed_records(response, chunk)
                LOGGER.warning(
                    {"message": "failed to put some records into stream, retrying..."}
                )

                response = KINESIS.put_records(
                    StreamName=STREAM, Records=failed_records
                )

        except ClientError as c_err:
            error_code = c_err.response["Error"]["Code"]
            if error_code == "ValidationException":
                message = {
                    "message": "Error validating record before put in stream",
                    "reason": str(c_err),
                    "chunk_size": sys.getsizeof(chunk),
                    "chunk": chunk,
                }
                LOGGER.error(message)
            else:
                raise c_err

        LOGGER.info(response)


def get_failed_records(response, chunk):
    """Filters error records from Kinesis put_records response.
    Arguments:
        response {dict} -- The response from put_records call.
        chunk {list} -- Records that were attempted to be put into a stream.
    Returns:
        list -- The filtered list of records which failed to be put into the stream.
    """
    return [
        chunk[idx]
        for idx, record in enumerate(response["Records"])
        if record.get("ErrorCode")
    ]


def handle_client_error(c_err, record):
    """
        handle client error when storing to dynamodb
    """
    error_code = c_err.response["Error"]["Code"]
    message = {error_code: {"reason": str(c_err)}}

    if error_code in RETRY_EXCEPTIONS:
        message[error_code].update({"record": record})
        LOGGER.error(message)
        raise c_err

    if error_code in IGNORE_EXCEPTIONS:
        message[error_code].update({"message": "ignored"})
        message[error_code].update({"record": record})
        LOGGER.warning(message)

    else:
        raise c_err


def cast_to_int(obj):
    """Cast decimal values to ints for offering response to avoid decimals
    when creating GSI keys for querying sale events table
    """
    if isinstance(obj, Decimal):
        return int(obj)
    return obj
