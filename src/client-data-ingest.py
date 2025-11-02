import boto3
from environs import Env
from botocore.exceptions import ClientError
from codeguru_profiler_agent import with_lambda_profiler

from aws_xray_sdk.core import patch_all, xray_recorder
from rpp_lib.logs import LOGGER
from voluptuous import Any, MultipleInvalid

from dynamodb.store import put_work_order
from utils.decode_record import decode_record
from utils.sqs import send_message


patch_all()

ENV = Env()

DYNAMO = boto3.resource("dynamodb")
SQS = boto3.client("sqs")
DL_QUEUE = SQS.get_queue_url(QueueName=ENV("DL_QUEUE", validate=Any(str)))["QueueUrl"]
PROFILE_GROUP = name = ENV("AWS_CODEGURU_PROFILER_GROUP_NAME", validate=Any(str))
RECON_WORK_ORDER_TABLE = DYNAMO.Table(ENV("WORKORDER_AM_TABLE", validate=Any(str)))


@with_lambda_profiler(profiling_group_name=PROFILE_GROUP)
@xray_recorder.capture()
def process_stream(event, _):
    """
    processing rpp-client-data-ingest kinesis stream for PO records uploaded by customer
    """

    LOGGER.debug({"event": event})

    decoded_records = [decode_record(record) for record in event["Records"] if record]

    for record in decoded_records:
        if record["eventName"] == "REMOVE":
            return

        try:
            po_record = record["dynamodb"]["NewImage"]
            workorder = po_record.pop("pk").split(":")[1]
            sk = po_record.pop("sk")
            put_work_order(workorder=workorder, sk=sk, record=po_record)

        except MultipleInvalid as validation_error:
            LOGGER.error(
                {
                    "message": "Record processing will be skipped",
                    "reason": str(validation_error),
                    "record": record,
                }
            )
            record.update({"reason": str(validation_error)})
            send_message(DL_QUEUE, record)

        except ClientError as db_err:
            message = {
                "event": "Client error",
                "reason": str(db_err),
                "record": record,
            }
            LOGGER.error(message)

            record.update({"reason": str(db_err)})
            send_message(DL_QUEUE, record)

        except Exception as err:
            message = {
                "event": "Unknown error",
                "reason": str(err),
                "record": record,
            }
            LOGGER.exception(message)

            record.update({"reason": str(err)})
            send_message(DL_QUEUE, record)
