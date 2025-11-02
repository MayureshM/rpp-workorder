from voluptuous import Any, Required, Optional, Schema, REMOVE_EXTRA
from decimal import Decimal


def parts_inventory_validator():
    return Schema(
        {
            Required("pk", msg="pk not found"): Any(
                str, msg="PartsInventory record has invalid pk"
            ),
            Required("sk", msg="sk not found"): Any(
                str, msg="PartsInventory record has invalid sk"
            ),
            Required("event_type", msg="event_type not found"): Any(
                str, msg="PartsInventory record has invalid event_type"
            ),
            Required("site_id", msg="site_id not found"): Any(
                str, msg="PartsInventory record has invalid site_id"
            ),
            Required("sblu", msg="sblu not found"): Any(
                str, msg="PartsInventory record has invalid sblu"
            ),
            Optional("order_number"): Any(
                str, msg="PartsInventory record has invalid order_number"
            ),
            Optional("order_header_status"): Any(
                str, msg="PartsInventory record has invalid order_header_status"
            ),
            Optional("order_type"): Any(
                str, msg="PartsInventory record has invalid order_type"
            ),
            Optional("order_total_amount"): Any(
                Decimal, int, msg="PartsInventory record has invalid order_total_amount"
            ),
            Optional("order_source"): Any(
                str, msg="PartsInventory record has invalid order_source"
            ),
            Optional("order_source_reference"): Any(
                str, msg="PartsInventory record has invalid order_source_reference"
            ),
            Optional("order_sold_to_customer_number"): Any(
                str, msg="PartsInventory record has invalid order_sold_to_customer_number"
            ),
            Optional("order_sold_to_customer_name"): Any(
                str, msg="PartsInventory record has invalid order_sold_to_customer_name"
            ),
            Optional("order_ship_from_org_name"): Any(
                str, msg="PartsInventory record has invalid order_ship_from_org_name"
            ),
            Optional("order_purchase_location"): Any(
                str, msg="PartsInventory record has invalid order_purchase_location"
            ),
            Optional("order_vin"): Any(
                str, msg="PartsInventory record has invalid order_vin"
            ),
            Optional("order_work_order_number"): Any(
                str, msg="PartsInventory record has invalid order_work_order_number"
            ),
            Optional("order_intended_use"): Any(
                str, msg="PartsInventory record has invalid order_intended_use"
            ),
            Required("order_line_number", msg="order_line_number not found"): Any(
                int, msg="PartsInventory record has invalid order_line_number"
            ),
            Optional("order_line_item"): Any(
                str, msg="PartsInventory record has invalid order_line_item"
            ),
            Optional("order_line_user_item_description"): Any(
                str, None, msg="PartsInventory record has invalid order_line_user_item_description"
            ),
            Optional("order_line_status"): Any(
                str, msg="PartsInventory record has invalid order_line_status"
            ),
            Optional("order_line_quantity"): Any(
                int, msg="PartsInventory record has invalid order_line_quantity"
            ),
            Optional("order_line_unit_price"): Any(
                int, Decimal, msg="PartsInventory record has invalid order_line_unit_price"
            ),
            Optional("order_line_amount"): Any(
                int, Decimal, msg="PartsInventory record has invalid order_line_amount"
            ),
            Optional("order_line_source_reference"): Any(
                str, msg="PartsInventory record has invalid order_line_source_reference"
            ),
            Optional("order_line_purchasing_source"): Any(
                str, None, msg="PartsInventory record has invalid order_line_purchasing_source"
            ),
            Optional("order_line_purchasing_category"): Any(
                str, None, msg="PartsInventory record has invalid order_line_purchasing_category"
            ),
            Optional("order_line_technician_name"): Any(
                str, None, msg="PartsInventory record has invalid order_line_technician_name"
            ),
            Required("updated", msg="updated not found"): Any(
                Decimal, str, msg="PartsInventory record has invalid updated"
            )
        },
        extra=REMOVE_EXTRA,
    )


def purchase_order_validator():
    return Schema(
        {
            Required("pk", msg="pk not found"): Any(
                str, msg="PurchaseOrder record has invalid pk"
            ),
            Required("sk", msg="sk not found"): Any(
                str, msg="PurchaseOrder record has invalid sk"
            ),
            Required("po_number", msg="po_number not found"): Any(
                str, msg="PurchaseOrder record has invalid purchase_order_number"
            ),
            Required("requisition", msg="requisition not found"): Any(
                str, msg="PurchaseOrder record has invalid resource"
            ),
            Required("part_line_id", msg="part_line_id not found"): Any(
                str, msg="PurchaseOrder record has invalid part_line_id"
            ),
            Required("site_id", msg="site_id not found"): Any(
                str, msg="PurchaseOrder record has invalid site_id"
            ),
            Required("sblu", msg="sblu not found"): Any(
                str, msg="PurchaseOrder record has invalid sblu"
            ),
            Required("event_type", msg="event_type not found"): Any(
                str, msg="PurchaseOrder record has invalid event_type"
            ),
            Optional("change_type", msg="change_type not found"): Any(
                str, msg="PurchaseOrder record has invalid change_type"
            ),
            Required("updated", msg="updated not found"): Any(
                float, int, Decimal, msg="PurchaseOrder record has invalid updated"
            )
        },
        extra=REMOVE_EXTRA,
    )


def validate_parts_inventory(event):
    validator = parts_inventory_validator()
    return validator(event)


def validate_purchase_order(event):
    validator = purchase_order_validator()
    return validator(event)
