import os

import json
from decimal import Decimal
import base64
import boto3
from dynamodb_json import json_util

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
from environs import Env
from botocore.exceptions import ClientError
from codeguru_profiler_agent import with_lambda_profiler
from rpp_lib.logs import LOGGER
from voluptuous import MultipleInvalid

from utils import sqs
from utils.common import get_updated_hr, get_utc_now
from dynamodb.store import put_work_order
from validation import valid_recon_approval, valid_recon_approval_item

patch_all()

RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")
IGNORE_EXCEPTIONS = "ConditionalCheckFailedException"

ENV = Env()
DL_QUEUE = ENV("DL_QUEUE", None)
DYNAMO = boto3.resource("dynamodb")
PROFILE_GROUP = os.environ.get("AWS_CODEGURU_PROFILER_GROUP_NAME")
TABLE = DYNAMO.Table(ENV("WORKORDER_AM_TABLE"))


@xray_recorder.capture()
@with_lambda_profiler(profiling_group_name=PROFILE_GROUP)
def process_recon_approval(event, _):
    """
    processing kinesis stream
    """

    LOGGER.info({"event": event})

    if "Records" in event.keys():
        for record in event["Records"]:
            process_record(record)
    else:
        process_record(event)


def process_record(record):
    key_event = ""
    dynamodb_event = None

    try:
        if record["eventSource"] == "aws:sqs":
            record = json.loads(record["body"])

        kinesis_event = json.loads(
            base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
        )

        dynamodb_event = json.loads(
            json.dumps(json_util.loads(kinesis_event)), parse_float=Decimal
        )

        key_event = "".join(dynamodb_event["dynamodb"]["Keys"].keys())

        new_image = dynamodb_event["dynamodb"]["NewImage"]
        pk = new_image["pk"].split(":")[1]
        sk = new_image["sk"]
        recon_approval = build_recon_approval(new_image, sk)
        utc_now = get_utc_now()
        recon_approval["updated"] = Decimal(utc_now.timestamp())
        recon_approval["updated_hr"] = get_updated_hr(utc_now)
        message = {"pk": pk, "sk": sk, "recon_approval": recon_approval}

        LOGGER.info(message)
        put_work_order(pk, sk, recon_approval)

    except MultipleInvalid as validation_error:
        LOGGER.error(
            {
                "message": "Validation error: Record processing will be skipped",
                "reason": str(validation_error),
                "record": record,
            }
        )
        sqs.send_message(DL_QUEUE, record)
    except KeyError as key_error:
        LOGGER.error(
            {
                "key_error": str(key_error),
                "key_event": key_event,
                "action": "Key error: skipping record",
                "dynamodb_event": dynamodb_event,
            }
        )
        sqs.send_message(DL_QUEUE, record)
    except ClientError:
        raise
    except Exception as err:
        record.update({"reason": str(err)})
        message = {
            "event": "Unknown error",
            "reason": str(err),
            "record": record,
        }

        LOGGER.exception(message)
        sqs.send_message(DL_QUEUE, record)


def build_recon_approval(new_record, sk):
    """
    build recon_approval dictionary
    """
    try:
        if "item_reference_id" in new_record:
            recon_approval = valid_recon_approval_item(new_record)
        else:
            recon_approval = valid_recon_approval(new_record)
    except MultipleInvalid as validation_error:
        message = {
            "message": "Not a valid recon approval event",
            "error": str(validation_error),
            "event": new_record,
        }

        LOGGER.error(message)
        raise
    return recon_approval
