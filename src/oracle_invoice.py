import base64
import json
import boto3
from botocore.exceptions import ClientError
from codeguru_profiler_agent import with_lambda_profiler
from dynamodb.store import put_work_order
from aws_lambda_powertools import Tracer, Logger
from environs import Env
from voluptuous import Any, MultipleInvalid
from dynamodb_json import json_util
from decimal import Decimal
from camel_converter.decorators import dict_to_snake


import utils.constants as c
from utils.sqs import send_message
from utils.common import (
    get_updated_hr,
    get_utc_now,
)
from validator.oracle_invoice import validate_payment


ENV = Env()
TRACER = Tracer()
LOGGER = Logger()
SQS = boto3.client("sqs")
DYNAMO = boto3.resource("dynamodb")
DL_QUEUE = ENV("DL_QUEUE")
PROFILE_GROUP = ENV("AWS_CODEGURU_PROFILER_GROUP_NAME", validate=Any(str))
TABLE = DYNAMO.Table(
    name=ENV("WORKORDER_TABLE", validate=Any(str))
)


@with_lambda_profiler(profiling_group_name=PROFILE_GROUP)
@TRACER.capture_lambda_handler(capture_response=False)
def handler(event, _):
    LOGGER.info({"DynamoDB event": event})

    for record in event["Records"]:
        try:
            single_record = json.loads(
                json.dumps(
                    json_util.loads(
                        json.loads(
                            base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
                        )
                    )
                ),
                parse_float=Decimal,
            )
            add_tracer_metadata_to_current_subsegment(single_record)

            LOGGER.info({"DynamoDB single_record": single_record})
            if single_record["eventName"] == "REMOVE":
                dynamodb = single_record["dynamodb"]
                pk = dynamodb["Keys"]["pk"]
                sk = dynamodb["Keys"]["sk"]
                TABLE.delete_item(
                    Key={"pk": pk, "sk": sk}
                )
            else:
                invoice = single_record["dynamodb"]["NewImage"]

                annotation_data = {
                    "pk": invoice["pk"],
                    "sk": invoice["sk"],
                    "site_id": invoice.get("auction_id", ""),
                    "work_order_number": invoice.get("work_order", ""),
                    "vin": invoice.get("vin", ""),
                }
                add_tracer_annotation_to_current_subsegment(annotation_data)

                """
                Getting the pk off of the inventory object and removing the first part of it.
                If the pk starts with workorder we are going to remove it so the put_work_order function
                does not duplicate the workorder:
                Also, checks if there is any other prefix to remoove
                else it will just send the base pk to the put_workorder function
                """
                if invoice["pk"].find(":") != -1:
                    workorder_pk = invoice.pop("pk").split(":")[1]
                else:
                    workorder_pk = invoice.pop("pk")

                LOGGER.debug({"workorder pk": workorder_pk})
                # Getting sk off of the invoice_id object
                sk = invoice.pop("sk")

                # adding the eventSource to make finding the objects in the database easier
                # added checking to see what event was the trigger
                if invoice.get("macola_invoice_date", ""):
                    invoice["eventSource"] = "macola"
                elif invoice.get("eventType", "") in c.EVENTER_VALID_ORACLE_TYPES:
                    invoice["eventSource"] = "oracle"

                if sk.startswith("CONSIGNMENT.BUYER_CONSIGNMENT_PAYMENT_CHANGED") or \
                        sk.startswith("CONSIGNMENT.BUYER_CONSIGNMENT_CHANGED"):
                    invoice_reference_id_exists = False
                    if "oracleInvoice" in invoice:
                        oracle_invoice = camelcase_snakecase(invoice.pop("oracleInvoice"))

                        try:
                            oracle_invoice_payments = validate_payment(oracle_invoice)
                            LOGGER.info({
                                "oracle_invoice_payments": oracle_invoice_payments,
                            })
                            oracle_invoice_consignments = oracle_invoice_payments["customer"]["consignments"][0]
                            payments = oracle_invoice_consignments["invoices"]
                            consignment_id = oracle_invoice_consignments["consignment_id"]
                            consignment_location = oracle_invoice_consignments.get("consignment_location", "")
                            consignment_reference_id = oracle_invoice_consignments.get("consignment_reference_id", "")
                            consignment_reference_id2 = oracle_invoice_consignments.get("consignment_reference_id2", "")
                            consignment_vin = oracle_invoice_consignments.get("consignment_vin", "")
                            consignment_payment_status = (
                                "PAID"
                                if all(
                                    payment.get("invoice_details", {}).get("payment_status", "") == "PAID" for payment in payments
                                )
                                else "UNPAID"
                            )
                            for payment in payments:
                                if payment["invoice_source"] != "INVOICE":
                                    LOGGER.info({"message": "invoice record not processed. Skipping. "
                                                            "inovice_source is not in INVOICE state",
                                                 "invoice_source": f"{payment['invoice_source']}",
                                                 "invoice_reference_id": f"{payment['invoice_reference_id']}",
                                                 "pk": workorder_pk,
                                                 "sk": sk})
                                    continue
                                if "invoice_reference_id" not in payment:
                                    LOGGER.info({"message": "invoice_reference_id is not available in the invoice"
                                                            " record. So, we are going to skip the invoice record",
                                                 "pk": workorder_pk,
                                                 "sk": sk})
                                    continue
                                invoice_reference_id_exists = True
                                invoice_reference_id = payment["invoice_reference_id"]
                                payment_sk = f"oracleinvoiceconsignmentdetails:{consignment_id}#{invoice_reference_id}"
                                payment["consignment_id"] = consignment_id
                                payment["consignment_location"] = consignment_location
                                payment["consignment_reference_id"] = consignment_reference_id
                                payment["consignment_reference_id2"] = consignment_reference_id2
                                payment["consignment_payment_status"] = consignment_payment_status
                                payment["consignment_vin"] = consignment_vin
                                site_id = workorder_pk.split('#')[-1]
                                sblu = workorder_pk.split('#')[0].split(':')[-1]
                                payment["site_id"] = site_id
                                payment["sblu"] = sblu
                                payment["vin"] = consignment_vin

                                utc_now = get_utc_now()
                                payment["updated"] = Decimal(utc_now.timestamp())
                                payment["updated_hr"] = get_updated_hr(utc_now)
                                payment_item = put_work_order(workorder=workorder_pk, sk=payment_sk, record=payment)
                                LOGGER.info({
                                    "payment": payment_item
                                })
                        except MultipleInvalid as mul_err:
                            LOGGER.exception({"message": "Validation Error in oracle invoice processing.",
                                              "oracle_invoice": f"{oracle_invoice}",
                                              "Error": f"{str(mul_err)}"})
                    if invoice_reference_id_exists:
                        item = put_work_order(workorder=workorder_pk, sk=sk, record=invoice)
                        message = {
                            "message": "put_work_order worked",
                            "item": item,
                            "pk": workorder_pk,
                            "sk": sk,
                        }
                    else:
                        message = {"message": "invoice_reference_id is not available in any of the invoice records"
                                              " in the whole list. So, no data is stored to the table",
                                   "pk": workorder_pk, "sk": sk}
                    LOGGER.info(message)
                    continue
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
            add_tracer_exception_to_current_subsegment(db_err)
            LOGGER.exception(db_error_message)
            send_message(DL_QUEUE, single_record)

        except UnicodeDecodeError as u_exc:
            message = "Invalid stream data, ignoring"
            reason = str(u_exc)
            exception = exec
            response = "N/A"

            LOGGER.exception(
                {
                    "event": message,
                    "reason": reason,
                    "record": single_record,
                    "exception": exception,
                    "response": response,
                }
            )
            add_tracer_exception_to_current_subsegment(u_exc)

        except (TypeError, KeyError) as t_k_err:
            message = "Failed to update/delete the record"
            reason = t_k_err
            exception = exec
            response = "N/A"

            LOGGER.exception(
                {
                    "event": message,
                    "reason": reason,
                    "record": single_record,
                    "exception": exception,
                    "response": response,
                }
            )
            add_tracer_exception_to_current_subsegment(t_k_err)

        except Exception as error:
            error_message = {"reason": error, "record": single_record}
            add_tracer_exception_to_current_subsegment(error)
            LOGGER.error(error_message)
            send_message(DL_QUEUE, single_record)


def add_tracer_metadata_to_current_subsegment(request):
    # TODO: Move this to utils.common once all the lambdas are upgraded to python3.9 (KTLO work for rpp-workorder)

    try:
        current_subsegment = TRACER.provider.current_subsegment()
        current_subsegment.put_metadata("record", request)
    except AttributeError as attr_err:
        LOGGER.exception(
            {"message": "Failed to send metadata to xray", "reason": attr_err, "request": request}
        )


def add_tracer_annotation_to_current_subsegment(request):
    # TODO: Move this to utils.common once all the lambdas are upgraded to python3.9 (KTLO work for rpp-workorder)

    try:
        current_subsegment = TRACER.provider.current_subsegment()
        current_subsegment.put_annotation("source", "Recon Workorder")
        [current_subsegment.put_annotation(key, value) for key, value in request.items()]
    except AttributeError as attr_err:
        LOGGER.exception(
            {"message": "Failed to send annotation to xray", "reason": attr_err, "request": request}
        )


def add_tracer_exception_to_current_subsegment(error):
    # TODO: Move this to utils.common once all the lambdas are upgraded to python3.9 (KTLO work for rpp-workorder)

    try:
        current_subsegment = TRACER.provider.current_subsegment()
        current_subsegment.add_error_flag()
        current_subsegment.add_fault_flag()
        current_subsegment.apply_status_code(500)
        current_subsegment.add_exception(error, [], True)
    except AttributeError as attr_err:
        LOGGER.exception(
            {"message": "Failed to send exception to xray", "reason": attr_err, "error": error}
        )


@dict_to_snake
def camelcase_snakecase(obj) -> dict[str, str]:
    #  TODO: this work for python3.9 versions. So, put this in common once all the lambdas are upgraded
    return obj
