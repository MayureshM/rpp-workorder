from voluptuous import (
    ALLOW_EXTRA,
    Schema,
    Required,
    Optional,
    Any,
    All,
    Length
)
from decimal import Decimal


def validate_service_status_ingest_event(event):
    event_validator = get_service_status_ingest_event_validator()
    return event_validator(event)


def get_service_status_ingest_event_validator():
    schema = Schema(
        {
            Required("pk", msg="Missing pk"): Any(str),
            Required("sk", msg="Missing sk"): Any(str),
            Required("sblu", msg="Missing sblu"): All(str, Length(min=1)),
            Required("work_order_number", msg="Missing work order number"): All(str, Length(min=1)),
            Optional("vin", msg="Missing vin"): Any(str, Length(min=1)),
            Required("site_id", msg="Missing site id"): All(str, Length(min=1)),
            Required("shop_code", msg="Missing shop code"): Any(str),
            Required("shop_status_code", msg="Missing shop status code"): Any(str),
            Required("shop_status_description", msg="Missing shop status description"): Any(str),
            Optional("from_as400", msg="Missing from as400"): Any(bool),
            Optional("user_id", msg="Missing user id"): str,
            Required("updated", msg="Missing updated field"): Any(float, int, Decimal),
            Required("last_updated_source", msg="Missing last updated source"): Any(str),
            Required("updated_by", msg="Missing updated by"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema
