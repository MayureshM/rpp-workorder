'''RECON360 Flow for parts Ingest Validator'''
from voluptuous import ALLOW_EXTRA, Any, Required, Schema, Optional


def get_parts_ingest_event_validator():
    schema = Schema(
        {
            Required("eventName"): Any("INSERT", "MODIFY", "REMOVE"),
            Required("dynamodb"): Schema(
                {
                    Optional("NewImage"): Schema(
                        {
                            Required("pk"): Any(str),
                            Required("sk"): Any(str),
                            Required("event_name"): Any("INSERT", "MODIFY", "REMOVE"),
                            Required("site_id"): Any(str),
                            Required("sblu"): Any(str),
                        },
                        extra=True,
                    ),
                    Optional("OldImage"): Schema(
                        {
                            Required("pk"): Any(str),
                            Required("sk"): Any(str),
                            Required("event_name"): Any("INSERT", "MODIFY", "REMOVE"),
                            Required("site_id"): Any(str),
                            Required("sblu"): Any(str),
                        },
                        extra=ALLOW_EXTRA,
                    ),
                },
                extra=ALLOW_EXTRA,
            ),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def validate_parts_ingest_event(event):
    event_validator = get_parts_ingest_event_validator()
    return event_validator(event)
