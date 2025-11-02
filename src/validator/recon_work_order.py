"""
File for handling validation for recon_work_order events
"""
import json
from voluptuous import REMOVE_EXTRA, ALLOW_EXTRA, All, Any, Schema, Required, Optional, DefaultTo
from decimal import Decimal


def find_validator():
    return Schema(
        {
            Required("key", msg="key is required"): Schema(
                {
                    Optional("pk"): All(str),
                    Optional("sk"): All(str),
                    Optional("site_id"): All(str),
                    Optional("manheim_account_number"): All(str),
                    Optional("work_order_number"): All(str),
                    Optional("vin"): All(str),
                },
                extra=ALLOW_EXTRA
            ),
            Optional("index"): All(str),
        },
        extra=REMOVE_EXTRA,
    )


def process_labor_status_validator():
    schema = Schema(
        {
            Required("pk"): Any(str),
            Required("sk"): Any(str),
            Required("current_status"): Schema(
                {
                    Required("date"): Any(str),
                    Required("source"): Any(str),
                    Required("labor_status"): Any(str),
                    Optional("updated_by"): Any(str),
                    Optional("labor_status_old"): Any(str),
                },
                extra=REMOVE_EXTRA,
            ),
            Required("updated"): Any(Decimal),
            Optional("actionCode"): Any(str),
            Optional("severityCode"): Any(str),
            Optional("charge_l_status"): Schema(
                {
                    Required("ecrvcf_status"): Any(str),
                    Required("translated_status"): Any(str),
                    Required("shop_code"): Any(str),
                },
                extra=REMOVE_EXTRA,
            ),
            Optional("charge_p_status"): Schema(
                {
                    Required("ecrvcf_status"): Any(str),
                    Required("translated_status"): Any(str),
                    Required("shop_code"): Any(str),
                },
                extra=REMOVE_EXTRA,
            ),
            Optional("sblu"): Any(str),
            Optional("site_id"): Any(str),
            Optional("work_order_number"): Any(str),
            Optional("item_code"): Any(str),
            Optional("damage"): Any(str),
            Optional("item"): Any(str),
            Optional("severity"): Any(str),
            Optional("action"): Any(str),
        },
        extra=REMOVE_EXTRA,
    )
    return schema


def get_damage_labor(labor_type):
    schema = Schema(
        {
            Required("action"): Any(str),
            Optional("action_code"): Any(str),
            Required("approved"): Any(True),
            Required("damage"): Any(str),
            Required("damage_code"): Any(str),
            Required("item"): Any(str),
            Required("item_code"): Any(str),
            Required(labor_type + "_labor_cost"): lambda v: Decimal(v),
            Required(labor_type + "_labor_hours"): lambda v: Decimal(v),
            Optional("repair_completion_date"): Any(str),
            Optional("severity_code"): Any(str, None),
            Required("shop_code"): Any(str),
            Required("shop_description"): Any(str),
            Optional("sub_item_code"): Any(str),
        },
        extra=REMOVE_EXTRA,
    )
    return schema


def validate_find_work_order(event):
    validator = find_validator()
    return validator(event)


def validate_process_labor_status(event):
    validator = process_labor_status_validator()
    return validator(event)


def valid_damage_labor(event, labor_type):
    validator = get_damage_labor(labor_type)
    return validator(event)


def get_conditions_by_vin():
    return Schema(
        {
            Required("vin"): All(str),
            Optional("work_order_number"): All(str),
            Optional("site_id"): All(str),
        },
        extra=REMOVE_EXTRA,
    )


def get_primary_key():
    return Schema(
        {
            Required("pk"): All(str),
        },
        extra=REMOVE_EXTRA,
    )


def validate_get_conditions_by_vin(event):
    validator = get_conditions_by_vin()
    return validator(event)


def validate_primary_key(event):
    validator = get_primary_key()
    return validator(event)


def get_condition_validator():
    return Schema(
        {
            Required("condition", msg="condition is required"): Schema(
                {
                    Required("damages", msg="damages are required"): Any(list),
                    Required("tires", msg="tires are required"): Any(list),
                },
                extra=REMOVE_EXTRA
            )
        },
        extra=REMOVE_EXTRA
    )


def validate_condition(event):
    validator = get_condition_validator()
    return validator(event)


def get_damage_validator():
    return Schema(
        {
            Required("id"): lambda v: int(v),
            Required("itemCode"): Any(str),
            Required("subItemCode"): Any(str),
            Optional("action"): Any(str),
            Optional("damage"): Any(str),
            Optional("severity"): Any(str),
            Optional("repairLaborHours"): lambda v: Decimal(v),
            Optional("paintLaborHours"): lambda v: Decimal(v),
            Optional("partLaborHours"): lambda v: Decimal(v),
            Optional("repairLaborCost"): lambda v: Decimal(v),
            Optional("paintLaborCost"): lambda v: Decimal(v),
            Optional("partLaborCost"): lambda v: Decimal(v),
            Optional("item"): Any(str),
            Optional("partDescription"): lambda v: str(v),
            Optional("partCost"): lambda v: Decimal(v),
            Optional("partQuantity"): Any(str),
            Optional("finalPartCost"): lambda v: Decimal(v),
            Optional("shopCode"): Any(str, None),
            Optional("shopDescription"): Any(str),
            Optional("imageHref"): Any(str),
            Optional("notes"): Any(str, None)
        },
        extra=REMOVE_EXTRA
    )


def validate_damage(event):
    validator = get_damage_validator()
    return validator(event)


def get_tire_validator():
    return Schema(
        {
            Required("location"): Any(str),
            Optional("depth"): lambda v: int(v),
            Optional("manufacturer"): Any(str),
            Optional("size"): Any(str)
        },
        extra=REMOVE_EXTRA
    )


def validate_tire(event):
    validator = get_tire_validator()
    return validator(event)


def get_damage_request_validator():
    return Schema(
        {
            Required("httpMethod", msg="not a valid APIGW request"): All(
                "POST", msg="expected a POST request"
            ),
            Required("body"): All(
                DefaultTo("{}"),
                lambda v: json.loads(v),
                Schema(
                    {
                        Required("site_id"): Any(str),
                        Required("work_order_number"): Any(str),
                        Required("item_code"): Any(str),
                        Required("damage"): Any(str)
                    },
                    extra=REMOVE_EXTRA
                ),
            ),
        },
        extra=ALLOW_EXTRA,
    )


def validate_workorder_damage(event):
    validator = get_damage_request_validator()
    return validator(event)
