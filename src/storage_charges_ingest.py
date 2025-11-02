from boto3 import resource, client
import utils.constants as c
from aws_xray_sdk.core import patch_all, xray_recorder
from botocore.exceptions import ClientError
from codeguru_profiler_agent import with_lambda_profiler
import datetime
from decimal import Decimal
from environs import Env
from rpp_lib.logs import LOGGER

from dynamodb.store import update_document_for_pk_and_sk
from utils.common import get_utc_now, get_updated_hr
from utils.decode_record import decode_record
from utils.sqs import send_message
from voluptuous import Any, MultipleInvalid

patch_all()

ENV = Env()

DYNAMO = resource("dynamodb")
SQS = client("sqs")
DL_QUEUE = SQS.get_queue_url(QueueName=ENV("DL_QUEUE", validate=Any(str)))["QueueUrl"]
PROFILE_GROUP = name = ENV("AWS_CODEGURU_PROFILER_GROUP_NAME", validate=Any(str))
RECON_WORK_ORDER_TABLE = DYNAMO.Table(ENV("WORKORDER_AM_TABLE", validate=Any(str)))

ANNOTATION_LIST = [
    c.APPROVER_ID,
    c.AUCTION_ID,
    c.CUSTOMER,
    c.RIMS_SKEY,
    c.SBLU,
    c.VIN,
    c.WORK_ORDER,
]

records_dictionary = {}


def convert_to_timestamp(date_str: str) -> int:
    date_obj = datetime.datetime.fromisoformat(date_str)
    return int(date_obj.timestamp())


def set_storage_date(records_dict: dict, pk: str, new_image: dict):
    utc_now = get_utc_now()
    payload = {
        "storage_date": new_image["storage_start_date"],
        "updated": Decimal(utc_now.timestamp()),
        "updated_hr": get_updated_hr(utc_now),
    }
    condition_expression = "attribute_not_exists(#storage_date) OR #storage_date <> :storage_date"

    try:
        update_document_for_pk_and_sk(pk, pk, payload, condition_expression=condition_expression)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ConditionalCheckFailedException':
            # That means we have already processed this record with the same storage date
            LOGGER.info(
                {
                    "message": "Record already processed with same storage date",
                    "pk": pk,
                    "storage_date": new_image["storage_start_date"],
                }
            )

    records_dict[pk] = {"storage_date": new_image["storage_start_date"]}


@with_lambda_profiler(profiling_group_name=PROFILE_GROUP)
@xray_recorder.capture()
def process_stream(event, _):
    """
    processing rpp-charges-ingest kinesis stream for storage charge items
    """

    LOGGER.info({"event": event})

    decoded_records = [decode_record(record) for record in event["Records"] if record]

    for record in decoded_records:
        LOGGER.info({"message": " Processing record.", "record": record})
        try:
            old_image = record["dynamodb"].get("OldImage", {})
            new_image = record["dynamodb"]["NewImage"]

            pk = record["dynamodb"]["Keys"]["pk"]

            # If the pk and storage date are already in the records_dictionary,
            # we will skip processing this record.
            if pk in records_dictionary and \
                    records_dictionary[pk]["storage_date"] == new_image["storage_start_date"]:
                LOGGER.info(
                    {
                        "message": "Record processing will be skipped",
                        "reason": "Record already processed with same storage date",
                        "record": record,
                    }
                )
                continue

            if "is_invalidated" in new_image and new_image["is_invalidated"]:
                """
                is_invalidated field is present in the new image and set to True.
                If this is the case, we will skip processing this record.
                """
                LOGGER.info(
                    {
                        "message": "Record processing will be skipped",
                        "reason": "is_invalidated is set to True",
                        "record": record,
                    }
                )
                continue

            if record["eventName"] == "INSERT":
                """
                If a new record was inserted, verify that "is_invalidated" is not set to False.
                """
                set_storage_date(records_dictionary, pk, new_image)

            elif record["eventName"] == "MODIFY":
                """
                If a record was modified, check if the storage_start_date has moved to the future or past.
                """
                new_image_timestamp = convert_to_timestamp(new_image["storage_start_date"])
                old_image_timestamp = convert_to_timestamp(old_image.get("storage_start_date", "1970-01-01"))
                if new_image_timestamp >= old_image_timestamp:
                    if not new_image.get("is_invalidated", False):
                        set_storage_date(records_dictionary, pk, new_image)
                else:
                    if new_image.get("is_invalidated", False) != old_image.get("is_invalidated", False) and \
                            not new_image.get("is_invalidated", False):
                        set_storage_date(records_dictionary, pk, new_image)

            else:
                """
                If the event is not INSERT or MODIFY, we will skip processing
                """
                LOGGER.info(
                    {
                        "message": "Record processing will be skipped",
                        "reason": "Event is not INSERT or MODIFY",
                        "record": record,
                    }
                )
                continue

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
