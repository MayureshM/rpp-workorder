"""
Constants
"""
import os

# common
WORKORDER_TABLE = os.getenv("WORKORDER_TABLE")
CUSTOMER_TABLE = os.getenv("CUSTOMER_TABLE")

JSON_HEADER = {"Content-Type": "application/json"}

# capture events
CAPTURE_COMPLETE = "COMPLETED"

# orders_condition
INCOMPLETE_CR = "NOT_COMPLETED"
COMPLETE_CR = "COMPLETED"
CONDITION_REQUESTED = "CR/SO"  # vcf category for condition report/sign off
VALID_TYPES = [
    "VALUE_STANDARD",
    "DELUXE_FRONT_LINE_READY",
    "WASH_AND_VAC",
    "RETAIL_READY",
    "DELUXE_SPONSORED_PLUS",
    "DELUXE_EXTRA_STEP",
    "PARTIAL_DETAIL",
    "ULTRA_DETAIL",
]

# cert_events
CERT_CREATED_EVENT = "ORDERS.CERTIFICATION.CREATED"
CERT_UPDATED_EVENT = "ORDERS.CERTIFICATION.UPDATED"
CERT_TYPE_YES = "CERT_YES"
CERT_TYPE_NO = "CERT_NO"
CERT_STATUS_REQUESTED = "REQUESTED"
CERT_STATUS_COMPLETED = "COMPLETED"

HEADERS = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}
MEASUREMENT_LOCATION = {
    "LF": "left_front",
    "LR": "left_rear",
    "RF": "right_front",
    "RR": "right_rear",
}
MEASUREMENT_TYPE = {
    "Brake Shoes/Pads": "brake",
    "Drum": "drum",
    "Rotor": "rotor",
    "Tire": "tire",
    "Battery": "battery",
}

# charge ingest constants
APPROVER_ID = "approver_id"
ASSOCIATED_CHARGE_KEYS = "associated_charge_keys"
AUCTION_ID = "auction_id"
APPROVED_WORK = "approved_work"
CHARGE_KEY = "charge_key"
CUSTOMER = "customer"
REPAIR_COST_CENTER = "repair_cost_center"
REQUIRED_CHARGES_STUB = "Charges record missing required field:"
RIMS_SKEY = "rims_skey"
SBLU = "sblu"
SITE_ID = "site_id"
STORAGE_DATE = "storage_date"
TRANSACTION_COMPLETE = "transaction_complete"
TYPE = "type"
UPDATED = "updated"
VALIDATOR_CHARGES_STUB = "Charges record has invalid"
VEHICLE_COMPLETE_FLAG = "vehicle_complete_flag"
VEHICLE_COMPLETE_STAGE = "vehicle_complete_stage"
VIN = "vin"
WORK_ORDER = "work_order"
WORK_ORDER_CREATE_DATE = "work_order_create_date"
EVENT_SOURCE_SMART_INSPECT = "SMART_INSPECT"
EVENT_SOURCE_AUCTION_ECR = "AUCTION_ECR"

# oracle and orbit event types
ORACLE_EVENT_TYPE_BUYER_CHANGED = "CONSIGNMENT.BUYER_CONSIGNMENT_CHANGED"
ORACLE_EVENT_TYPE_PAYMENT_CHANGED = "CONSIGNMENT.BUYER_CONSIGNMENT_PAYMENT_CHANGED"

EVENTER_VALID_ORACLE_TYPES = [
    ORACLE_EVENT_TYPE_BUYER_CHANGED,
    ORACLE_EVENT_TYPE_PAYMENT_CHANGED,
]
