import os

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from botocore.exceptions import ClientError
from codeguru_profiler_agent import with_lambda_profiler
from dynamodb.store import put_work_order
from rpp_lib.logs import LOGGER

from utils.decode_record import decode_record
from utils.sqs import send_message

patch_all()

SQS = boto3.client("sqs")
DYNAMO = boto3.resource("dynamodb")
DL_QUEUE = SQS.get_queue_url(QueueName=os.environ["DL_QUEUE"])["QueueUrl"]
PROFILE_GROUP = os.environ.get("AWS_CODEGURU_PROFILER_GROUP_NAME")
TABLE = DYNAMO.Table(os.environ["WORKORDER_TABLE"])


@xray_recorder.capture()
@with_lambda_profiler(profiling_group_name=PROFILE_GROUP)
def handler(event, _):
    LOGGER.info({"DynamoDB event": event})

    decoded_records = [decode_record(record) for record in event["Records"] if record]

    LOGGER.debug({"decoded messages": decoded_records})

    for single_record in decoded_records:
        try:
            invoice: dict = single_record["dynamodb"]["NewImage"]

            if invoice["pk"].find(":") != -1:
                workorder_pk = invoice.pop("pk").split(":")[1]
            else:
                workorder_pk = invoice.pop("pk")

            LOGGER.debug({"workorder pk": workorder_pk})
            # Getting sk off of the invoice_id object
            sk = invoice.pop("sk")

            # adding the eventSource to make finding the objects in the database easier
            # added checking to see what event was the trigger
            invoice["eventSource"] = "orbit"

            item = put_work_order(workorder=workorder_pk, sk=sk, record=invoice)
            message = {
                "message": "put_work_order worked",
                "item": item,
                "pk": workorder_pk,
                "sk": sk,
            }
            LOGGER.info(message)

        except ClientError as db_err:
            db_error_message = {
                "event": "Client Error",
                "reason": db_err,
                "record": single_record,
            }
            LOGGER.error(db_error_message)
            send_message(DL_QUEUE, single_record)

        except Exception as error:
            error_message = {"reason": error, "record": single_record}
            LOGGER.error(error_message)
            send_message(DL_QUEUE, single_record)
