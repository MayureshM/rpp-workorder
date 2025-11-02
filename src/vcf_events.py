"""
   order vcf_events functions
"""

from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
from dateutil import parser
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import Any, MultipleInvalid

from validation import (
    valid_vcf_ad_hoc_created,
    valid_vcf_ad_hoc_removed,
    valid_vcf_ad_hoc_updated,
    valid_vcf_event_completed,
    valid_vcf_event_created,
)

ENV = Env()
DYNAMO = boto3.resource("dynamodb")
CATEGORY_TABLE = DYNAMO.Table(name=ENV("CATEGORY_TABLE", validate=Any(str)))

AD_HOC_DAMAGE = (
    ("MECH", "AP"),
    ("MECH", "M"),
    ("BODY", "BA"),
    ("BODY", "B"),
    ("GLASS", "W"),
    ("AB", "AB"),
    ("WETSD", "WS"),
    ("TOUCH", "TU"),
    ("TRIM", "TR"),
    ("PARTS", "PN"),
    ("KEYS", "KA"),
)

AD_HOC_DESCRIPTION = {
    "MECH": "MECH",
    "BODY": "BODY / PAINT",
    "GLASS": "WINDSHIELD / GLASS",
    "AB": "AIR BRUSH",
    "WETSD": "WET/SAND/BUFF",
    "TOUCH": "TOUCH UP",
    "TRIM": "TRIM",
    "PARTS": "PARTS",
    "KEYS": "KEYS",
}


def get_category_data(key):
    """
        get vcf_events category info from category table
    """
    item = CATEGORY_TABLE.get_item(Key={"key": key})["Item"]
    result = {
        "shop": item["category"],
        "shop_description": item["shopDescription"],
        "labor_hours": item["hours"],
        "labor_name": item["laborName"],
        "product_id": item["productId"],
    }

    return result


def build_order(new_record):
    """
        build vcf_events order dictionary
    """

    order = None
    event_type = None
    vcf_events_service = {
        "vcf_events_id": new_record["vcf_events_id"],
        "labor_status": "Ready for Repair",
    }

    LOGGER.info({"vcf_events_service": vcf_events_service})

    try:
        vcf_events_service.update(valid_vcf_event_created(new_record["order"]))
        request_date = parser.parse(vcf_events_service["id"]["vlshdt"]).isoformat()
        order = get_category_data("UCFIN")
        order.update({"retail_inspection_request_date": request_date + "Z"})
        order.update({"vcf_events_service": vcf_events_service})
        event_type = "UCFIN"
        LOGGER.info({"vcf_events_order": order})

    except MultipleInvalid as validation_error:
        message = {
            "message": "not updated vcf event, trying completed",
            "reason": str(validation_error),
            "event": new_record,
        }
        LOGGER.warning(message)
        try:
            vcf_events_service.update(valid_vcf_event_completed(new_record["order"]))
            complete_date = parser.parse(vcf_events_service["id"]["vlshdt"]).isoformat()
            order = get_category_data("UCFIN")
            order.update({"vcf_events_service": vcf_events_service})
            order.update({"labor_status": "Completed"})
            order.update({"complete_date": complete_date + "Z"})
            event_type = "UCFIN"

            LOGGER.info({"vcf_events_order_2": order})

        except MultipleInvalid as validation_error:
            message = {
                "message": "not completed vcf event, trying create ad hoc damage",
                "reason": str(validation_error),
                "event": new_record,
            }
            LOGGER.warning(message)
            try:
                record = valid_vcf_ad_hoc_created(new_record, AD_HOC_DAMAGE)
                approval_date = (
                    f"{parser.parse(record['order']['id']['vlshdt']).isoformat()}Z"
                )
                order = {
                    "ad_hoc_damage": True,
                    "current_status": {
                        "date": f"{record['order']['cdctimestamp']}Z",
                        "labor_status": record["labor_status"],
                        "source": "VCF",
                        "updated_by": record["order"]["updatedby"],
                    },
                    "initial_approval_date": approval_date,
                    "name": record["name"],
                    "shop_code": record["ad_hoc"],
                    "shop_description": AD_HOC_DESCRIPTION[record["ad_hoc"]],
                    "recent_approval_date": approval_date,
                    "updated": Decimal(record["updated"]),
                    "vcf_event": record["order"],
                }
                if record.get("labor_status_old"):
                    order["current_status"]["labor_status_old"] = record.get(
                        "labor_status_old"
                    )

                event_type = "AD_HOC_CREATED"
            except MultipleInvalid as validation_error:
                message = {
                    "message": "not ad hoc damage create, trying ad hoc damage update",
                    "reason": str(validation_error),
                    "event": new_record,
                }
                LOGGER.warning(message)
                try:
                    record = valid_vcf_ad_hoc_updated(new_record)
                    approval_date = (
                        f"{parser.parse(record['order']['id']['vlshdt']).isoformat()}Z"
                    )
                    order = {
                        "current_status": {
                            "date": f"{record['order']['cdctimestamp']}Z",
                            "labor_status": record["labor_status"],
                            "source": "VCF",
                            "updated_by": record["order"]["updatedby"],
                        },
                        "name": record["name"],
                        "recent_approval_date": approval_date,
                        "shop_code": record["ad_hoc"],
                        "shop_description": AD_HOC_DESCRIPTION[record["ad_hoc"]],
                        "updated": Decimal(record["updated"]),
                        "vcf_event": record["order"],
                    }

                    if record.get("labor_status_old"):
                        order["current_status"]["labor_status_old"] = record.get(
                            "labor_status_old"
                        )

                    if order["current_status"]["labor_status"].upper() == "COMPLETED":
                        order["current_status"]["complete_date"] = order[
                            "current_status"
                        ]["date"]

                    event_type = "AD_HOC_UPDATED"

                except MultipleInvalid as validation_error:
                    message = {
                        "message": "not ad hoc damage updated, trying ad hoc damage remove",
                        "reason": str(validation_error),
                        "event": new_record,
                    }
                    LOGGER.warning(message)
                    try:
                        record = valid_vcf_ad_hoc_removed(new_record)
                        order = {
                            "shop_code": record["ad_hoc"],
                        }
                        event_type = "AD_HOC_REMOVED"
                    except MultipleInvalid as validation_error:
                        message = {
                            "message": "Ignoring vcf_events event, "
                            "did not match any known vcf_events event types",
                            "reason": str(validation_error),
                            "event": new_record,
                        }
                        LOGGER.warning(message)
    return order, event_type


def get_vcf_events(new_image):
    """
        return order vcf_events column
    """

    column = None
    try:
        data, event_type = build_order(new_image)
        if data is not None and event_type == "UCFIN":
            column = {"name": "ucfin", "updated_name": "ucfin_vcf_event", "data": data}
        elif data is not None:
            column = {
                "event_type": event_type,
                "data": data,
                "store_wo_record": store_wo_record,
            }

    except KeyError as k_error:
        message = {
            "message": "Bad vcfevents event",
            "reason": str(k_error),
            "event": new_image,
        }
        if "order" in str(k_error):
            LOGGER.debug(message)
        else:
            LOGGER.error(message)

    return column


def build_expressions(column, updated):
    """
        Build expressions for the store wo record function
    """

    if column["event_type"] == "AD_HOC_CREATED":
        params = {
            "ConditionExpression": "attribute_not_exists(condition_service) OR "
            "condition_service.#condition_service_status <> :condition_service_status",
            "ExpressionAttributeNames": {
                "#ad_hoc": "ad_hoc_damages",
                "#condition_service_status": "labor_status",
                "#damage": column["data"]["shop_code"],
                "#recent_approval": "ad_hoc_recent_approval_date",
            },
            "ExpressionAttributeValues": {
                ":condition_service_status": "Completed",
                ":damage": column["data"],
                ":recent": column["data"]["recent_approval_date"],
            },
            "UpdateExpression": "set #ad_hoc.#damage = :damage, #recent_approval = :recent",
        }

    elif column["event_type"] == "AD_HOC_UPDATED":
        params = {
            "ConditionExpression": "attribute_exists(ad_hoc_damages.#damage) AND "
            "ad_hoc_damages.#damage.#updated <= :updated",
            "ExpressionAttributeNames": {f"#{k}": f"{k}" for k in column["data"]},
            "ExpressionAttributeValues": {
                f":{k}": v for k, v in column["data"].items()
            },
            "UpdateExpression": f"set {', '.join(f'ad_hoc_damages.#damage.#{k} = :{k}' for k in column['data'])}",
        }
        params["ExpressionAttributeNames"].update(
            {
                "#damage": column["data"]["shop_code"],
                "#recent_approval": "ad_hoc_recent_approval_date",
            }
        )
        params["ExpressionAttributeValues"].update(
            {":recent": column["data"]["recent_approval_date"]}
        )
        params["UpdateExpression"] += ", #recent_approval = :recent"

    else:
        params = {
            "ExpressionAttributeNames": {"#damage": column["data"]["shop_code"]},
            "UpdateExpression": "remove ad_hoc_damages.#damage",
        }

    return params


# pylint: disable=too-many-locals, too-many-arguments
def store_wo_record(new_image, updated, column, unit, table, new_column=False):
    """
        Store workorder service to dynamodb
    """
    key = {
        "work_order_key": new_image["vcf_events_id"],
        "site_id": new_image["site_id"],
    }
    params = build_expressions(column, updated)

    params.update({"Key": key, "ReturnValues": "UPDATED_NEW"})

    LOGGER.debug({"message": "store record via the following", "parameters": params})

    response = None
    try:
        response = table.update_item(**params)
    except ClientError as c_err:
        error_code = c_err.response["Error"]["Code"]
        if (
            error_code == "ValidationException"
            and column["event_type"] == "AD_HOC_CREATED"
        ):
            response = table.update_item(
                Key=key,
                ConditionExpression="attribute_not_exists(condition_service) OR "
                "condition_service.#condition_service_status <> :condition_service_status",
                ExpressionAttributeNames={
                    "#account_number": "manheim_account_number",
                    "#ad_hoc": "ad_hoc_damages",
                    "#check_in_date": "check_in_date",
                    "#company_name": "company_name",
                    "#condition_service_status": "labor_status",
                    "#group_code": "group_code",
                    "#init_approval": "ad_hoc_initial_approval_date",
                    "#recent_approval": "ad_hoc_recent_approval_date",
                    "#sblu": "sblu",
                    "#vin": "vin",
                    "#work_order_number": "work_order_number",
                },
                ExpressionAttributeValues={
                    ":account_number": new_image["consignment"]["manheimAccountNumber"],
                    ":ad_hoc": {column["data"]["shop_code"]: column["data"]},
                    ":check_in_date": new_image["consignment"]["checkInDate"],
                    ":company_name": unit["contact"]["companyName"],
                    ":condition_service_status": "Completed",
                    ":group_code": unit["account"]["groupCode"],
                    ":init": column["data"]["initial_approval_date"],
                    ":recent": column["data"]["recent_approval_date"],
                    ":sblu": new_image["sblu"],
                    ":vin": new_image["vin"],
                    ":work_order_number": new_image["work_order_number"],
                },
                UpdateExpression="set #ad_hoc = :ad_hoc, #recent_approval = :recent, "
                "#init_approval = :init, #account_number = :account_number, #vin = :vin, "
                "#sblu = :sblu, #company_name = :company_name, "
                "#work_order_number = :work_order_number, #group_code = :group_code, "
                "#check_in_date = :check_in_date",
            )
        else:
            raise c_err

    return response
