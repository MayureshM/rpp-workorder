from voluptuous import (
    Any,
    Required,
    Optional,
    Schema,
    REMOVE_EXTRA,
    Length,
    All,
    ALLOW_EXTRA,
)


def str_convert(v):
    return (str(v)).lower()


def validate_payment_validator():
    return Schema(
        {
            Required("customer", msg="customer not found"): Schema({
                Required("consignments", msg="consignments not found"): All(
                    list,
                    Length(min=1, msg="At least one invoice line item is required"),
                    [
                        {
                            Required("consignment_id", msg="consignment_id not found"): Any(
                                str, msg="oracleInvoice record has invalid consignment_id"
                            ),
                            Optional("consignment_location", msg="consignment_location not found"): Any(
                                str, msg="oracleInvoice record has invalid consignment_location"
                            ),
                            Required("consignment_reference_id", msg="consignment_reference_id not found"): Any(
                                str, msg="oracleInvoice record has invalid consignment_reference_id"
                            ),
                            Optional("consignment_reference_id2", msg="consignment_reference_id2 not found"): Any(
                                str, msg="oracleInvoice record has invalid consignment_reference_id2"
                            ),
                            Optional("consignment_vin", msg="consignment_vin not found"): Any(
                                str, msg="oracleInvoice record has invalid consignment_vin"
                            ),
                            Required("invoices", msg="invoices not found"): All(
                                list,
                                Length(min=1, msg="At least one invoice line item is required"),
                                [
                                    {
                                        Required("invoice_source", msg="invoice_source - field not found"):
                                        All(str, Length(min=1), msg="invoice_source field cannot be empty"),
                                        Required("invoice_id", msg="invoice_id - field not found"):
                                        All(str, Length(min=1), msg="invoice_id field cannot be empty"),
                                        Required("invoice_number", msg="invoice_number - field not found"):
                                        All(str, Length(min=1, msg="invoice_number field cannot be empty")),
                                        Optional("invoice_reference_id",
                                                 msg="invoice_reference_id - field not found"):
                                        All(str, Length(min=1), msg="invoice_reference_id field cannot be empty")
                                    }
                                ], extra=ALLOW_EXTRA
                            )
                        }
                    ], extra=REMOVE_EXTRA),
            }, extra=ALLOW_EXTRA)
        },
        extra=REMOVE_EXTRA,
    )


def validate_payment(event):
    validator = validate_payment_validator()
    return validator(event)
