"""
Enhanced notes validation schema and functions
"""

from voluptuous import Any, Schema, Required, Optional, ALLOW_EXTRA


def get_enhanced_notes_validation_schema():
    """
    Enhanced validation schema for workorder notes
    """
    schema = Schema(
        {
            Required("eventName"): Any("INSERT"),
            Required("dynamodb"): Schema(
                {
                    Optional("NewImage"): Schema(
                        {
                            Required("pk", msg="PK is required"): Any(
                                str, msg="PK must be a string"
                            ),
                            Required("sk", msg="SK is required"): Any(
                                str, msg="SK must be a string"
                            ),
                            Required("note", msg="Note is required"): Any(
                                str, msg="Note must be a string"
                            ),
                            Required("sblu", msg="SBLU is required"): Any(
                                str, msg="SBLU must be a string"
                            ),
                            Required("site_id", msg="Site ID is required"): Any(
                                str, msg="Site ID must be a string"
                            ),
                            Required("source", msg="Source is required"): Any(
                                str, msg="Source must be a string"
                            ),
                            Required("type", msg="Type is required"): Any(
                                str, msg="Type must be a string"
                            ),
                            Optional("user_id", default=""): Any(
                                str, msg="User ID must be a string"
                            ),
                            Optional("user_name", default=""): Any(
                                str, msg="User Name must be a string"
                            ),
                            Required("vin", msg="VIN is required"): Any(
                                str, msg="VIN must be a string"
                            ),
                            Required("work_order_number", msg="work_order_number is required"): Any(
                                str, msg="work_order_number must be a string"
                            ),
                        },
                        extra=ALLOW_EXTRA,
                    ),
                    Optional("ApproximateCreationDateTime"): Any(float, int),
                },
                extra=ALLOW_EXTRA,
            ),
        },
        extra=ALLOW_EXTRA,
    )
    return schema


def valid_enhanced_notes_item(event):
    """
    Validate enhanced notes item
    """
    validator = get_enhanced_notes_validation_schema()
    return validator(event)
