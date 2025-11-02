import boto3
import utils.constants as c
from aws_xray_sdk.core import patch_all, xray_recorder
from botocore.exceptions import ClientError
from codeguru_profiler_agent import with_lambda_profiler
from decimal import Decimal
from dynamodb.store import put_work_order, update_document_for_pk_and_sk
from environs import Env
from rpp_lib.logs import LOGGER
from utils.common import get_removed_attributes, get_utc_now, get_updated_hr
from utils.decode_record import decode_record
from utils.sqs import send_message
from voluptuous import Any, MultipleInvalid

patch_all()

ENV = Env()

DYNAMO = boto3.resource("dynamodb")
SQS = boto3.client("sqs")
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


def set_manheim_account_number(
    records_dict: dict, pk: str, manheim_account_number: str
):
    """
    Update manheim_account_number in DynamoDB for the given pk

    Args:
        records_dict: Dictionary to track processed records
        pk: Primary key for the DynamoDB record
        mainheim_account_number: manheim_account_number to be updated
    """
    utc_now = get_utc_now()
    payload = {
        "manheim_account_number": manheim_account_number,
        "updated": Decimal(utc_now.timestamp()),
        "updated_hr": get_updated_hr(utc_now),
    }
    condition_expression = "attribute_not_exists(#manheim_account_number) OR #manheim_account_number <> :manheim_account_number"

    try:
        update_document_for_pk_and_sk(
            pk, pk, payload, condition_expression=condition_expression
        )
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ConditionalCheckFailedException":
            # That means we have already processed this record with the same manheim_account_number
            LOGGER.info(
                {
                    "message": "Record already processed with same manheim_account_number",
                    "pk": pk,
                    "manheim_account_number": manheim_account_number,
                }
            )

    records_dict[pk] = {"manheim_account_number": manheim_account_number}


@with_lambda_profiler(profiling_group_name=PROFILE_GROUP)
@xray_recorder.capture()
def process_stream(event, _):
    """
    processing rpp-charges-ingest kinesis stream for charge items
    """

    LOGGER.info({"event": event})

    decoded_records = [decode_record(record) for record in event["Records"] if record]
    records_dict = {}  # Track processed records for manheim_account_number updates

    for record in decoded_records:
        LOGGER.info({"message": " Processing record.", "record": record})
        try:
            charge_call = record["dynamodb"]["NewImage"]
            remove_attributes = []
            old_image = record["dynamodb"].get("OldImage", {})

            if old_image:
                remove_attributes = get_removed_attributes(
                    charge_call.keys(), old_image.keys(), record_mapping=None
                )

            """
            define sk for vehicle release events
            """
            workorder = charge_call.pop("pk").split(":")[1]
            sk = charge_call.pop("sk")

            # Check if we should update manheim_account_number
            pk_for_update = f"workorder:{workorder}"
            new_manheim_account_number = record["dynamodb"]["NewImage"][
                "manheim_account_number"
            ]

            if (
                sk.startswith("charge")
                and "manheim_account_number" in record["dynamodb"]["NewImage"]
                and pk_for_update
                not in records_dict  # Not already processed in this batch
                and (
                    not old_image
                    or old_image.get("manheim_account_number")
                    != new_manheim_account_number
                )
            ):

                LOGGER.info(
                    {
                        "message": f"Updating manheim_account_number for workorder={workorder}, sk={sk}",
                        "new_manheim_account_number": new_manheim_account_number,
                        "old_manheim_account_number": (
                            old_image.get("manheim_account_number")
                            if old_image
                            else None
                        ),
                    }
                )

                try:
                    set_manheim_account_number(
                        records_dict, pk_for_update, new_manheim_account_number
                    )
                except Exception as update_err:
                    LOGGER.error(
                        {
                            "message": "Failed to update manheim_account_number",
                            "workorder": workorder,
                            "error": str(update_err),
                        }
                    )

            LOGGER.info(
                {
                    "message": f"Adding a charge document into the table rpp-recon-work-order with  workorder={workorder} and sk={sk}, remove_attributes={remove_attributes}"
                }
            )
            put_work_order(
                workorder=workorder,
                sk=sk,
                remove_attributes=remove_attributes,
                record=charge_call,
            ),
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
