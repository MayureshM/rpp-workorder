from datetime import datetime
from decimal import Decimal

from dynamodb_json import json_util
from voluptuous import REMOVE_EXTRA, Any, Optional, Required, Schema

from dynamodb.store import get_work_oder_index

ALLOWED_LOT = (998, 999)


class InvalidDspRecordException(Exception):
    pass


def amazon_dsp_validator():
    return Schema({
        Required("pk"): Any(str),
        Required("sk"): Any(str),
        Optional("amz_requested_delivery_date"): Any(str),
        Optional("date_hub_unit_assigned_to_dsp"): Any(str),
        Optional("delivery_address"): Any(str),
        Optional("delivery_packet_status"): Any(str),
        Optional("del_city"): Any(str),
        Optional("del_state"): Any(str),
        Optional("del_zip"): Any(str, int),
        Optional("dsp_contact_name"): Any(str),
        Optional("dsp_contact_email"): Any(str),
        Optional("dsp_contact_phone"): Any(str),
        Optional("launching"): Any(str),
        Optional("manheim_account_number"): Any(str),
        Optional("originating_dsp_breakdown"): Any(str),
        Optional("originating_dsp_name"): Any(str),
        Optional("paperwork_status"): Any(str),
        Optional("payer_id"): Any(str, int),
        Optional("recv_dsp"): Any(str),
        Optional("recv_station"): Any(str),
        Optional("redeployment_receiving_unit"): Any(str, int),
        Optional("redeployment_year"): Any(str, int),
        Optional("redeployment_make"): Any(str),
        Optional("redeployment_model"): Any(str),
        Optional("redeployment_vehicle_type"): Any(str),
        Optional("redeployment_receiving_breakdown_notadj"): Any(str),
        Optional("redeployment_receiving_name"): Any(str),
        Required("redeployment_receiving_vin"): Any(str),
        Optional("redeployment_receiving_dsp_delivery_date_actual"): Any(str),
        Optional("regional_hub"): Any(str),
        Required("sblu"): Any(str),
        Required("site_id"): Any(str),
        Optional("times_through"): Any(str, int),
        Required("transport_partner_location"): Any(str),
        Optional("transport_partner"): Any(str),
        Optional("t_r_completion_date"): Any(str),
        Optional("t_r_request_date"): Any(str),
        Optional("t_r_submitted_to_ele"): Any(str),
        Optional("total_loss_status"): Any(str),
        Required("uid"): Any(str),
        Optional("unit_status"): Any(str),
        Required("vin"): Any(str),
        Optional("vta_sent_date"): Any(str),
        Optional("vta_signed_date"): Any(str),
        Required("work_order_number"): Any(str, int),
    }, extra=REMOVE_EXTRA,
    )


def amazon_dsp_manheim_validator():
    """
        Manheim Inventory files(4992734)
    """
    return Schema({
        Required("pk"): Any(str),
        Required("sk"): Any(str),
        Optional("amazon_requested_delivery_date"): Any(str),
        Optional("assignment_status"): Any(str),
        Optional("assignment_status_date"): Any(str),
        Optional("client"): Any(str),
        Optional("current_breakdown"): Any(str),
        Optional("current_breakdown_name"): Any(str),
        Optional("current_driver_address_line_1"): Any(str),
        Optional("current_driver_address_line_2"): Any(str),
        Optional("current_driver_breakdown"): Any(str),
        Optional("current_driver_city"): Any(str),
        Optional("current_driver_county"): Any(str),
        Optional("current_driver_email_address"): Any(str),
        Optional("current_driver_employee_id"): Any(str),
        Optional("current_driver_first_name"): Any(str),
        Optional("current_driver_full_name"): Any(str),
        Optional("current_driver_home_phone"): Any(str),
        Optional("current_driver_last_name"): Any(str),
        Optional("current_driver_name"): Any(str),
        Optional("current_driver_state_province"): Any(str),
        Optional("current_driver_status"): Any(str),
        Optional("current_driver_work_phone"): Any(str, int),
        Optional("current_driver_zip_postal_code"): Any(str),
        Optional("current_month_depreciation_amount"): Any(str),
        Optional("delivery_date_review"): Any(str),
        Optional("exterior_color"): Any(str),
        Optional("factory_order_date"): Any(str),
        Optional("gross_vehicle_weight_rating"): Any(str, int, Decimal),
        Optional("hub_vendor"): Any(str),
        Optional("last_mile_vendor"): Any(str),
        Optional("make"): Any(str),
        Optional("model"): Any(str),
        Optional("model_code"): Any(str),
        Optional("model_year"): Any(str),
        Optional("mso_received_date"): Any(str),
        Optional("mso_sent_date"): Any(str),
        Optional("na"): Any(str),
        Optional("payer_id"): Any(str, int),
        Optional("priority"): Any(str),
        Required("sblu"): Any(str),
        Required("site_id"): Any(str),
        Optional("ship_to_code"): Any(str),
        Optional("ship_to_code_added_date"): Any(str),
        Optional("title_date"): Any(str),
        Optional("title_in_house_indicator"): Any(str),
        Optional("title_issue_state_province"): Any(str),
        Optional("tr_vendor"): Any(str),
        Required("unit"): Any(str, int),
        Optional("vehicle_license_plate"): Any(str),
        Optional("vehicle_license_plate_state_province"): Any(str),
        Required("vin"): Any(str),
        Required("work_order_number"): Any(str, int),
    }, extra=REMOVE_EXTRA,
    )


def validate_dsp_constraints(wo_number: str):
    """Validator for dsp records based on sellers
    - lot_location must be in (998, 999) (ALLOWED_LOT)
    - current active task should be Storage Onsite

    Args:
        wo_number: work order number
    """
    items = get_work_oder_index({
                                "work_order_number": str(wo_number)},
                                "index_work_order_number")
    active_tasks = []
    storage_onsite_completedOn = None
    for item in items:
        item = json_util.loads(item, parse_float=Decimal)
        if item["sk"] == "pfvehicle:body":
            if int(item["lot_location"]) not in ALLOWED_LOT:
                raise InvalidDspRecordException(
                    f"DSP: Invalid lot_location: {item['lot_location']}"
                )
        elif item["sk"].startswith("active_task"):
            active_tasks.append(item)
        elif item["sk"].startswith("completed_task"):
            if "storage_onsite" in item["sk"]:
                # Saving time in case of loopback scenario
                storage_onsite_completedOn = datetime.strptime(
                    item["completedOn"], "%Y-%m-%dT%H:%M:%SZ"
                )

    active_task = get_current_active_task(active_tasks)
    at_createdOn = datetime.strptime(active_task["createdOn"], "%Y-%m-%dT%H:%M:%SZ")
    # Comparison of times in case of loopback scenario
    storage_onsite_completed = (
        storage_onsite_completedOn is not None
        and storage_onsite_completedOn >= at_createdOn
    )
    if (active_task["type"].strip() != "Storage Onsite"
            or storage_onsite_completed is True):
        raise InvalidDspRecordException(
            f"DSP: Invalid Active Task: {active_task} | Storage is completed."
        )


def get_current_active_task(active_tasks: list):
    """Get current active task for a work order

    Args:
        active_tasks: list of active tasks
    """
    active_tasks.sort(key=lambda x: x["updated"], reverse=True)
    return active_tasks[0]


def amazon_ingest_validator():
    return Schema(
        {
            Required("pk"): Any(str),
            Required("sk"): Any(str),
            Optional("actual_drop_off_date"): Any(str),
            Optional("actual_pickup_date"): Any(str),
            Optional("check_in_date"): Any(str, None),
            Optional("consignment_status"): Any(str),
            Optional("days_in_dispatch"): Any(Decimal, str, None),
            Optional("days_to_deliver"): Any(Decimal, str),
            Optional("distance_in_miles"): Any(Decimal, str),
            Optional("drop_off_address"): Any(str),
            Optional("drop_off_city"): Any(str),
            Optional("drop_off_location"): Any(str),
            Optional("drop_off_state"): Any(str),
            Optional("drop_off_zip"): Any(str),
            Optional("ebol_link"): Any(str),
            Optional("estimated_drop_off_date"): Any(str, None),
            Optional("estimated_pickup_date"): Any(str, None),
            Optional("expected_delivery_date"): Any(str, None),
            Optional("group_load_id"): Any(str, None),
            Optional("inventory_type"): Any(str, None),
            Required("invoice_number"): Any(str),
            Optional("is_inbound"): Any(bool, None),
            Optional("is_outbound"): Any(bool, None),
            Optional("last_attempt"): Any(str, None),
            Optional("manheim_account_number"): Any(str, int),
            Optional("make"): Any(str),
            Optional("model"): Any(str),
            Optional("next_call"): Any(str, None),
            Optional("order_created_date"): Any(str),
            Optional("order_cancel_date"): Any(str),
            Optional("order_source"): Any(str),
            Optional("payer_id"): Any(str),
            Optional("payer_name"): Any(str),
            Optional("pickup_address"): Any(str),
            Optional("pickup_city"): Any(str),
            Optional("pickup_location"): Any(str),
            Optional("pickup_state"): Any(str),
            Optional("pickup_zip"): Any(str),
            Optional("retail_price"): Any(Decimal, str),
            Required("sblu"): Any(str),
            Required("site_id"): Any(str),
            Required("shipper_id"): Any(str),
            Optional("shipper_name"): Any(str),
            Optional("status"): Any(str),
            Optional("unaccepted_delivery"): Any(str),
            Optional("vehicle_id"): Any(str),
            Required("vin"): Any(str),
            Required("work_order_number"): Any(str, int),
            Optional("year"): Any(str),
        }, extra=REMOVE_EXTRA,
    )


def amazon_transport_lp_validator():
    return Schema(
        {
            Required("pk"): Any(str),
            Required("sk"): Any(str),
            Optional("check_in_date"): Any(str, None),
            Required("client_number"): Any(int, str),
            Optional("calculated_eta"): Any(str),
            Optional("deployment_redeployment"): Any(str),
            Required("is_inbound"): Any(bool),
            Optional("model"): Any(str),
            Required("make"): Any(str),
            Required("manheim_account_number"): Any(str, int),
            Optional("plate_expiration_date"): Any(str),
            Optional("plate_number"): Any(str),
            Optional("plate_state"): Any(str),
            Optional("po_date"): Any(str),
            Required("po_number"): Any(str),
            Optional("request_cancel_date"): Any(str),
            Optional("request_canceled_reason"): Any(str),
            Required("sblu"): Any(str),
            Required("site_id"): Any(str),
            Optional("transportation_co_name"): Any(str),
            Optional("transport_hub_address_1"): Any(str),
            Optional("transport_hub_address_2"): Any(str),
            Optional("transport_hub_city"): Any(str),
            Optional("transport_hub_name"): Any(str),
            Optional("transport_hub_state"): Any(str),
            Optional("transport_hub_zip"): Any(int, str),
            Optional("unit"): Any(int, str),
            Required("vin"): Any(int, str),
            Required("work_order_number"): Any(str, int),
            Required("year"): Any(str)
        }, extra=REMOVE_EXTRA
    )


def validate_amazon_ingest(event):
    if event["sk"].startswith("transport_lp"):
        validator = amazon_transport_lp_validator()
    else:
        validator = amazon_ingest_validator()
    return validator(event)


def validate_amazon_dsp_ingest(event):
    if event["sk"].startswith("dsp_manheim"):
        validator = amazon_dsp_manheim_validator()
    else:
        validator = amazon_dsp_validator()
    return validator(event)
