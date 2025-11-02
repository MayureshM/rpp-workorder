from voluptuous import (
    ALLOW_EXTRA,
    Schema,
    Required,
    Optional,
    Any,
)
from decimal import Decimal


def validate_labor_ingest_event(event):
    event_validator = get_labor_ingest_event_validator(event)
    return event_validator(event)


def get_labor_ingest_event_validator(event):
    status = ("repair_labor_status" if "repair_labor_status" in event else "estimate_status")
    status_translated = (
        "repair_labor_status_translated" if "repair_labor_status_translated" in event else "estimate_status_translated"
    )

    schema = Schema(
        {
            Required("pk", msg="Missing pk"): str,
            Required("sk", msg="Missing sk"): str,
            Required("action_code", msg="Missing action code"): str,
            Required("damage_code", msg="Missing damage code"): str,
            Required("item_code", msg="Missing item code"): str,
            Required(
                "last_updated_source", msg="Missing last updated source"
            ): str,
            Required(
                status, msg=f"Missing {status} field"
            ): str,
            Required(
                status_translated,
                msg=f"Missing {status_translated} field",
            ): str,
            Required("sblu", msg="Missing sblu"): str,
            Required("site_id", msg="Missing site id"): str,
            Required("shop_code", msg="Missing shop code"): str,
            Required("sub_item_code", msg="Missing sub item code"): str,
            Required("severity", msg="Missing severity"): str,
            Required("severity_code", msg="Missing severity code"): str,
            Required(
                "work_order_number", msg="Missing work order number"
            ): str,
            Required("updated", msg="Missing updated field"): Any(
                int, Decimal
            ),
            Optional("updated_ecrvcf"): Any(int, Decimal),
            Optional("updated_wpe"): Any(int, Decimal),
            Required("updated_by", msg="Missing updated by"): str,
            Optional("user_id", msg="Missing user id"): str,
        },
        extra=ALLOW_EXTRA,
    )

    return schema
