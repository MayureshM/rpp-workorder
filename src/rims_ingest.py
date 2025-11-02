"""
processor for rpp-rims-ingest-events
"""
import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from codeguru_profiler_agent import with_lambda_profiler
from dynamodb.store import put_work_order
from environs import Env
from rpp_lib.logs import LOGGER
from utils.decode_record import decode_record
from voluptuous import Any
from utils.dynamodb import convert_to_date_stamp

patch_all()

RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")

IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"

ENV = Env()
DYNAMO = boto3.resource("dynamodb")
RECON_WORK_ORDER_TABLE = DYNAMO.Table(ENV("WORKORDER_AM_TABLE"))
KINESIS = boto3.client("kinesis")
PROFILE_GROUP = name = ENV("AWS_CODEGURU_PROFILER_GROUP_NAME", validate=Any(str))


@xray_recorder.capture()
def process_record(record):
    LOGGER.debug({"dynamo_record": record})
    xray_recorder.put_metadata("dynamodb_record", record)

    rims_record = record["dynamodb"]["NewImage"]

    xray_recorder.put_annotation("site_id", rims_record.get("auction_id", ""))
    xray_recorder.put_annotation("work_order_number", rims_record.get("workorder", ""))
    xray_recorder.put_annotation("work_order_key", rims_record.get("pk", ""))
    xray_recorder.put_annotation("vin", rims_record.get("vin", ""))
    xray_recorder.put_annotation("skey", rims_record.get("skey", ""))

    rims_record["sk"] = f"rims:{rims_record['skey']}"
    updated = rims_record["updated"]
    rims_record["hr_updated"] = convert_to_date_stamp(updated)
    workorder = rims_record.pop("pk").split(":")[1]
    sk = rims_record.pop("sk")
    put_work_order(workorder=workorder,
                   sk=sk,
                   record=rims_record)


@with_lambda_profiler(profiling_group_name=PROFILE_GROUP)
@xray_recorder.capture()
def process_stream(event, _):
    """
    processing kinesis stream
    """

    LOGGER.debug({"event": event})

    decoded_records = [decode_record(record) for record in event["Records"] if record]

    for record in decoded_records:
        process_record(record)
