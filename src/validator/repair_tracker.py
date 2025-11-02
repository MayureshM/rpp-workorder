"""
File for handling validation for recon_work_order events
"""
from voluptuous import REMOVE_EXTRA, Any, Schema, Required, Optional
from decimal import Decimal


def clocking_event_validator():
    return Schema(
        {
            Required("pk", msg="pk not found"): Any(
                str, msg="Clocking record has invalid pk"
            ),
            Required("sk", msg="sk not found"): Any(
                str, msg="Clocking record has invalid sk"
            ),
        },
        extra=True
    )


def validate_es_clocks_validator():
    return Schema(
        {
            Required("pk", msg="pk not found"): Any(
                str, msg="Clocking record has invalid pk"
            ),
            Required("sk", msg="sk not found"): Any(
                str, msg="Clocking record has invalid sk"
            ),
            Optional("clock_user", msg="clock_user not found"): Any(
                str, msg="Clocking record has invalid clock_user"
            ),
            Optional("phase_name", msg="phase_name not found"): Any(
                str, msg="Clocking record has invalid phase_name"
            ),
            Optional("work_order_number", msg="work_order_number not found"): Any(
                str, msg="Clocking record has invalid work_order_number"
            ),
            Optional("hr_updated", msg="hr_updated not found"): Any(
                str, msg="Clocking record has invalid hr_updated"
            ),
            Optional("initiator_task_name", msg="initiator_task_name not found"): Any(
                str, msg="Clocking record has invalid initiator_task_name"
            ),
            Optional("item_code", msg="item_code not found"): Any(
                str, msg="Clocking record has invalid item_code"
            ),
            Optional("sblu", msg="sblu not found"): Any(
                str, msg="Clocking record has invalid sblu"
            ),
            Optional("severity", msg="severity not found"): Any(
                str, msg="Clocking record has invalid severity"
            ),
            Optional("group_code", msg="group_code not found"): Any(
                str, msg="Clocking record has invalid group_code"
            ),
            Optional("action", msg="action not found"): Any(
                str, msg="Clocking record has invalid action"
            ),
            Optional("damage", msg="damage not found"): Any(
                str, msg="Clocking record has invalid damage"
            ),
            Optional("mod_user", msg="mod_user not found"): Any(
                str, msg="Clocking record has invalid mod_user"
            ),
            Optional("shop_code", msg="shop_code not found"): Any(
                str, msg="Clocking record has invalid shop_code"
            ),
            Optional("sub_item_code", msg="sub_item_code not found"): Any(
                str, msg="Clocking record has invalid sub_item_code"
            ),
            Optional("vin_last_6", msg="vin_last_6 not found"): Any(
                str, msg="Clocking record has invalid vin_last_6"
            ),
            Optional("task_name", msg="task_name not found"): Any(
                str, msg="Clocking record has invalid task_name"
            ),
            Optional("item", msg="item not found"): Any(
                str, msg="Clocking record has invalid item"
            ),
            Optional("recorded_time", msg="recorded_time not found"): Any(
                str, msg="Clocking record has invalid recorded_time"
            ),
            Optional("updated", msg="updated not found"): Any(
                Decimal, float, int, str, msg="Clocking record has invalid updated"
            ),
            Optional("damage_id", msg="damage_id not found"): Any(
                str, msg="Clocking record has invalid damage_id"
            ),
            Optional("clock_action", msg="clock_action not found"): Any(
                str, msg="Clocking record has invalid clock_action"
            ),
            Optional("vin", msg="vin not found"): Any(
                str, msg="Clocking record has invalid vin"
            ),
            Optional("seller_name", msg="seller_name not found"): Any(
                str, msg="Clocking record has invalid seller_name"
            ),
            Optional("seller_number", msg="seller_number not found"): Any(
                str, msg="Clocking record has invalid seller_number"
            ),
            Optional("site_id", msg="site_id not found"): Any(
                str, msg="Clocking record has invalid site_id"
            ),
            Optional("timestamp", msg="timestamp not found"): Any(
                Decimal, float, int, str, msg="Clocking record has invalid timestamp"
            ),
        },
        extra=REMOVE_EXTRA,
    )


def validate_es_clocks(event):
    validator = validate_es_clocks_validator()
    return validator(event)


def validate_clocking_event(event):
    validator = clocking_event_validator()
    return validator(event)
