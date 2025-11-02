from decimal import Decimal

from voluptuous import REMOVE_EXTRA, All, Any, NotIn, Optional, Required, Schema, In, Length
from voluptuous.error import Invalid
from voluptuous.schema_builder import ALLOW_EXTRA
from utils.constants import COMPLETE_CR, INCOMPLETE_CR


def is_complete():
    def is_complete_retailrecon(value):
        c_task = "Mechanical Inspection"
        if c_task not in [task["taskName"] for task in value["completedTasks"]]:
            raise Invalid(str(c_task) + " not found, not complete")

        a_task = "Diagnose"
        if a_task in [task["type"] for task in value["activeTasks"]]:
            raise Invalid(str(a_task) + " active task invalid, not complete")

        return value

    return is_complete_retailrecon


def get_approval():
    schema = Schema(
        {Required("createdOn"): Any(str), Required("updatedOn"): Any(str)},
        extra=ALLOW_EXTRA,
    )

    return schema


def get_damage():
    schema = Schema(
        {
            Required("action"): Any(str),
            Required("actionCode"): Any(str),
            Required("approved"): Any(True),
            Required("damage"): Any(str),
            Required("damageCode"): Any(str),
            Required("item"): Any(str),
            Required("itemCode"): Any(str),
            Optional("repairCompletionDate"): Any(str),
            Optional("repairLaborCost"): lambda v: Decimal(v),
            Optional("repairLaborHours"): lambda v: Decimal(v),
            Optional("paintLaborCost"): lambda v: Decimal(v),
            Optional("paintLaborHours"): lambda v: Decimal(v),
            Optional("partLaborCost"): lambda v: Decimal(v),
            Optional("partLaborHours"): lambda v: Decimal(v),
            Optional("severityCode"): Any(str, None),
            Optional("shopCode", default="MISC"): lambda shop_code: str(shop_code)
            if shop_code
            else "MISC",
            Optional("shopDescription", default="MISC"): lambda shop_desc: str(
                shop_desc
            )
            if shop_desc
            else "MISC",
            Required("subItemCode"): Any(str),
        },
        extra=REMOVE_EXTRA,
    )
    return schema


def get_damage_labor(labor_type):
    schema = Schema(
        {
            Required("action"): Any(str),
            Required("action_code"): Any(str),
            Required("approved"): Any(True),
            Required("damage"): Any(str),
            Required("damage_code"): Any(str),
            Required("item"): Any(str),
            Required("item_code"): Any(str),
            Required(labor_type + "_labor_cost"): Any(Decimal),
            Required(labor_type + "_labor_hours"): Any(Decimal),
            Optional("repair_completion_date"): Any(str),
            Optional("severity_code"): Any(str, None),
            Required("shop_code"): Any(str),
            Required("shop_description"): Any(str),
            Required("sub_item_code"): Any(str),
        },
        extra=REMOVE_EXTRA,
    )
    return schema


def get_new_image():
    schema = Schema(
        {
            Required("consignment"): Schema(
                {
                    Required("checkInDate"): Any(str),
                    Required("manheimAccountNumber"): Any(str),
                    Required("status"): NotIn("CHECKED_OUT"),
                },
                extra=ALLOW_EXTRA,
            )
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_consignment_event():
    schema = Schema(
        {
            Required("consignment"): Schema(
                {
                    Required("checkInDate"): Any(str),
                    Required("manheimAccountNumber"): Any(str),
                    Required("status"): Any(str),
                },
                extra=ALLOW_EXTRA,
            ),
            Required("site_id"): Any(str),
            Required("work_order_key"): Any(str),
            Required("vin"): Any(str),
            Required("sblu"): Any(str),
            Required("work_order_number"): Any(str),
        },
        extra=REMOVE_EXTRA,
    )
    return schema


def get_rejection():
    schema = Schema(
        {
            Required("create_time"): Any(str),
            Required("mod_user"): Any(str),
            Required("reject_details"): Any(str),
            Required("reject_reason"): Any(str),
            Required("reject_stage"): Any(str),
            Required("reject_status"): Any(str),
            Required("sblu"): Any(str),
            Required("work_order_number"): Any(str),
            Required("vin"): Any(str),
            Required("site_id"): Any(str),
            Required("checkin_date"): Any(str),
            Required("client_name"): Any(str),
            Required("client_number"): Any(str),
            Optional("lot_location"): Any(int, None),
            Optional("lot_name"): Any(str, None),
            Required("make"): Any(str),
            Required("model"): Any(str),
            Required("year"): Any(int),
            Optional("qc_outcome"): Any(str, None),
        },
        extra=REMOVE_EXTRA,
    )
    return schema


def get_retailrecon_updated():
    schema = Schema(
        {
            Required("activeTasks"): Schema(
                [
                    Schema(
                        {
                            Required("type"): Any(
                                "Vehicle Qualification", "Mechanical Inspection"
                            )
                        },
                        extra=ALLOW_EXTRA,
                    )
                ]
            ),
            Required("createdOn"): Any(str),
            Required("updatedOn"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_labor_fee_status():
    schema = Schema(
        {
            Required("labor_status_id"): Any(str),
            Required("labor"): Any("CERT", "DETL", "UCFIN"),
            Required("serial_id"): Any(str),
            Required("current_status"): Schema(
                {
                    Required("date"): Any(str),
                    Required("source"): Any(str),
                    Required("labor_status"): Any(str),
                    Optional("updated_by"): Any(str),
                    Optional("labor_status_old"): Any(str),
                    Optional("complete_date", default="Remove"): Any(str),
                },
                extra=REMOVE_EXTRA,
            ),
        },
        extra=REMOVE_EXTRA,
    )
    return schema


def get_labor_condition_status():
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
            Optional("ad_hoc_damage"): Any(bool),
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
        },
        extra=REMOVE_EXTRA,
    )
    return schema


def get_retailrecon_completed():
    is_valid = Schema(
        {
            Required("completedTasks"): Schema(
                [
                    Schema(
                        {
                            Required("taskName"): Any(str),
                            Required("completedOn"): Any(str, None),
                        },
                        extra=ALLOW_EXTRA,
                    )
                ]
            ),
            Required("createdOn"): Any(str),
            Required("updatedOn"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    is_completed = Schema(is_complete())

    return All(is_valid, is_completed)


def get_vcf_event_created():
    schema = Schema(
        {
            Required("id"): Schema(
                {Required("vlshdt"): Any(str), Required("vlfabr"): Any("UCFIN")},
                extra=ALLOW_EXTRA,
            ),
            Required("vlstat"): Any("UI"),
            Required("changestatus"): Any("I", "U"),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_vcf_event_completed():
    schema = Schema(
        {
            Required("id"): Schema(
                {Required("vlshdt"): Any(str), Required("vlfabr"): Any("UCFIN")},
                extra=ALLOW_EXTRA,
            ),
            Required("vlstat"): Any("WC", "01"),
            Required("changestatus"): Any("I", "U"),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_certification_updated():
    schema = Schema(
        {
            Required("type"): Any("CERT_YES", "IN_PROCESS"),
            Required("status"): NotIn("COMPLETED"),
            Required("legacyStatus"): Any("CI"),
            Required("createdOn"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_certification_completed():
    legacy_status = Schema(
        {
            Required("type"): Any("CERT_YES", "IN_PROCESS"),
            Required("status"): NotIn(
                ["COMPLETED", "CANCELED", "CANCELLED", "DECLINED"]
            ),
            Required("legacyStatus"): NotIn("CI"),
        },
        extra=ALLOW_EXTRA,
    )

    status = Schema(
        {
            Required("type"): Any("CERT_YES", "IN_PROCESS"),
            Required("status"): Any("COMPLETED"),
            Required("legacyStatus"): Any(str),
            Required("updatedOn"): Any(str),
            Required("createdOn"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return Any(status, legacy_status)


def get_certification_canceled():
    schema = Schema(
        {Required("status"): Any("CANCELED", "CANCELLED", "DECLINED")},
        extra=ALLOW_EXTRA,
    )

    return schema


def get_condition_updated():
    schema = Schema(
        {
            Required("type"): Any("CR/SO"),
            Required("status"): Any("NOT_COMPLETED", None),
            Required("createdTimestamp"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_condition_completed():
    schema = Schema(
        {
            Required("type"): Any("CR/SO"),
            Required("status"): Any("COMPLETED"),
            Required("completedTimestamp"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_detail_requested():
    schema = Schema(
        {
            Required("type"): Any(
                "VALUE_STANDARD",
                "DELUXE_FRONT_LINE_READY",
                "WASH_AND_VAC",
                "RETAIL_READY",
                "DELUXE_SPONSORED_PLUS",
                "DELUXE_EXTRA_STEP",
                "PARTIAL_DETAIL",
                "ULTRA_DETAIL",
            ),
            Required("status"): Any("REQUESTED"),
            Required("createdOn"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_detail_completed():
    schema = Schema(
        {
            Required("type"): Any(
                "VALUE_STANDARD",
                "DELUXE_FRONT_LINE_READY",
                "WASH_AND_VAC",
                "RETAIL_READY",
                "DELUXE_SPONSORED_PLUS",
                "DELUXE_EXTRA_STEP",
                "PARTIAL_DETAIL",
                "ULTRA_DETAIL",
                "OTHER",
            ),
            Required("status"): Any("COMPLETED"),
            Required("createdOn"): Any(str),
            Required("updatedOn"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_detail_declined():
    schema = Schema(
        {
            Required("type"): Any(
                "DELUXE_EXTRA_STEP",
                "DELUXE_FRONT_LINE_READY",
                "DELUXE_SPONSORED_PLUS",
                "NO_DETAIL",
                "PARTIAL_DETAIL",
                "RETAIL_READY",
                "ULTRA_DETAIL",
                "VALUE_STANDARD",
                "WASH_AND_VAC",
                "OTHER",
            ),
            Required("status"): Any("DECLINED"),
            Required("createdOn"): Any(str),
            Required("updatedOn"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_detail_canceled():
    schema = Schema(
        {
            Required("type"): Any(
                "DELUXE_EXTRA_STEP",
                "DELUXE_FRONT_LINE_READY",
                "DELUXE_SPONSORED_PLUS",
                "NO_DETAIL",
                "PARTIAL_DETAIL",
                "RETAIL_READY",
                "ULTRA_DETAIL",
                "VALUE_STANDARD",
                "WASH_AND_VAC",
                "OTHER",
            ),
            Required("status"): Any("CANCELED", "CANCELLED"),
            Required("createdOn"): Any(str),
            Required("updatedOn"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_offering_sold_updated():
    schema = Schema(
        {
            Required("status"): Any("SOLD"),
            Required("channel"): Any(str),
            Optional("saleYear"): Any(str, int),
            Optional("saleNumber"): Any(str, int),
            Optional("virtualLaneNumber"): Any(str, int),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_offering_in_lane_updated():
    schema = Schema(
        {
            Required("status"): Any("REQUESTED", "ACTIVE"),
            Required("channel"): Any("IN_LANE"),
            Required("saleYear"): Any(str, int),
            Required("saleNumber"): Any(str, int),
            Required("virtualLaneNumber"): Any(str, int),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_offering_canceled():
    schema = Schema(
        {Required("status"): Any("CANCELED", "CANCELLED")}, extra=ALLOW_EXTRA
    )

    return schema


def get_work_order_request():
    schema = Schema(
        {
            Required("work_order_number"): Any(str, None),
            Required("site_id"): Any(str, None),
            Optional("work_order_key"): Any(str, None),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_labor_category():
    schema = Schema(
        {
            Required("category"): Any(str, None),
            Required("key"): Any(str, None),
            Required("product_id"): Any(str, None),
            Required("shop_description"): Any(str, None),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_work_credit_condition():
    schema = Schema(
        {
            Optional("sublet_vendor_id", default="Remove"): Any(str),
            Optional("sublet_vendor_name", default="Remove"): Any(str),
            Required("damage_code"): Any(str),
            Required("item_code"): Any(str),
            Required("labor"): Any(str),
            Required("site_id"): Any(str),
            Required("sub_item_code"): Any(str),
            Required("work_credit"): Any(str),
            Required("work_order_key"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_work_credit_fee():
    schema = Schema(
        {
            Optional("sublet_vendor_id", default="Remove"): Any(str),
            Optional("sublet_vendor_name", default="Remove"): Any(str),
            Required("labor"): Any(str),
            Required("serial_id"): Any(str),
            Required("site_id"): Any(str),
            Required("work_credit"): Any(str),
            Required("work_order_key"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_work_credit():
    schema = Schema(
        {
            Required("work_credit"): Any(str),
            Optional("sublet_vendor_id"): Any(str),
            Optional("sublet_vendor_name"): Any(str),
        },
        extra=REMOVE_EXTRA,
    )

    return schema


def get_capture():
    schema = Schema(
        {
            Required("order"): Schema(
                {
                    Required("status"): Any(str),
                    Required("completedTimestamp"): Any(str),
                },
                extra=ALLOW_EXTRA,
            ),
            Required("work_order_key"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_order_condition_schema():
    return Schema(
        {
            Required("work_order_key"): Any(str),
            Required("site_id"): Any(str),
            Required("sblu"): Any(str),
            Required("work_order_number"): Any(str),
            Required("vin"): Any(str),
            Required("order"): Schema(
                {
                    Required("href"): Any(str),
                    Required("unit"): Schema({Required("href"): Any(str)}, extra=True),
                    Required("status"): In([COMPLETE_CR, INCOMPLETE_CR]),
                },
                extra=ALLOW_EXTRA,
            ),
        },
        extra=ALLOW_EXTRA,
    )


def is_ad_hoc_create(AD_HOC_TUPLE):
    def validate_ad_hoc(value):
        order = value.get("order", {})
        category = order.get("id", {}).get("vlfabr")
        status = order.get("vlstat")
        if (category, status) in AD_HOC_TUPLE:
            return value
        else:
            raise Invalid(
                f"Not a valid ad hoc creation combination {category}({status})"
            )

    return validate_ad_hoc


def get_vcf_ad_hoc_created(AD_HOC_TUPLE):
    schema = Schema(
        {
            Required("ad_hoc"): Any(str),
            Required("name"): Any(str),
            Required("labor_status"): Any(str),
            Required("updated"): Any(str),
            Required("sblu"): Any(str),
            Required("site_id"): Any(str),
            Required("vcf_events_id"): Any(str),
            Required("vin"): Any(str),
            Required("work_order_number"): Any(str),
            Required("consignment"): Schema(
                {
                    Required("checkInDate"): Any(str),
                    Required("manheimAccountNumber"): Any(str),
                },
                extra=ALLOW_EXTRA,
            ),
            Required("order"): Schema(
                {
                    Required("cdctimestamp"): Any(str),
                    Required("id"): Schema(
                        {Required("vlfabr"): Any(str), Required("vlshdt"): Any(str)},
                        extra=ALLOW_EXTRA,
                    ),
                    Required("updatedby"): Any(str),
                    Required("vlstat"): Any(str),
                    Required("vlvcde"): NotIn("VD"),
                    Optional("vlstatp"): Any(str),
                },
                extra=ALLOW_EXTRA,
            ),
            Optional("labor_status_old"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    ad_hoc_validator = Schema(is_ad_hoc_create(AD_HOC_TUPLE))

    return All(schema, ad_hoc_validator)


def get_vcf_ad_hoc_updated():
    schema = Schema(
        {
            Required("ad_hoc"): Any(str),
            Required("name"): Any(str),
            Required("labor_status"): Any(str),
            Required("updated"): Any(str),
            Required("site_id"): Any(str),
            Required("vcf_events_id"): Any(str),
            Required("order"): Schema(
                {
                    Required("cdctimestamp"): Any(str),
                    Required("id"): Schema(
                        {
                            Required("vlfabr"): Any(
                                "MECH",
                                "BODY",
                                "GLASS",
                                "AB",
                                "WETSD",
                                "TOUCH",
                                "TRIM",
                                "PARTS",
                                "KEYS",
                            ),
                            Required("vlshdt"): Any(str),
                        },
                        extra=ALLOW_EXTRA,
                    ),
                    Required("updatedby"): Any(str),
                    Required("vlstat"): Any(str),
                    Required("vlvcde"): NotIn("VD"),
                    Optional("vlstatp"): Any(str),
                },
                extra=ALLOW_EXTRA,
            ),
            Optional("labor_status_old"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_vcf_ad_hoc_removed():
    schema = Schema(
        {
            Required("ad_hoc"): Any(str),
            Required("name"): Any(str),
            Required("labor_status"): Any(str),
            Required("updated"): Any(str),
            Required("site_id"): Any(str),
            Required("vcf_events_id"): Any(str),
            Required("order"): Schema(
                {
                    Required("cdctimestamp"): Any(str),
                    Required("id"): Schema(
                        {
                            Required("vlfabr"): Any(
                                "MECH",
                                "BODY",
                                "GLASS",
                                "AB",
                                "WETSD",
                                "TOUCH",
                                "TRIM",
                                "PARTS",
                                "KEYS",
                            ),
                            Required("vlshdt"): Any(str),
                        },
                        extra=ALLOW_EXTRA,
                    ),
                    Required("updatedby"): Any(str),
                    Required("vlvcde"): Any("VD"),
                    Optional("vlstatp"): Any(str),
                },
                extra=ALLOW_EXTRA,
            ),
            Optional("labor_status_old"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_retail_inspection():
    schema = Schema(
        {
            Required("eventName"): Any("INSERT", "MODIFY", "REMOVE"),
            Required("dynamodb"): Schema(
                {
                    Optional("NewImage"): Schema(
                        {
                            Required("sk"): Any(str),
                            Required("pk"): Any(str),
                            Required("work_order_number"): Any(str),
                            Required("site_id"): Any(str),
                            Required("sblu"): Any(str),
                            Optional("mod_user"): Any(str),
                            Optional("vin"): Any(str),
                            Optional("status"): Any(str),
                            Optional("master_item_id"): Any(str),
                            Optional("item_type"): Any(str),
                            Optional("item_name"): Any(str),
                            Optional("ip_workorder_id"): Any(str),
                            Optional("inspection_item_id"): Any(str),
                            Optional("hr_updated"): Any(str),
                        },
                        extra=True,
                    ),
                },
                extra=True,
            ),
        },
        extra=True,
    )

    return schema


def get_retail_estimate():
    schema = Schema(
        {
            Required("eventName"): Any("INSERT", "MODIFY", "REMOVE"),
            Required("dynamodb"): Schema(
                {
                    Optional("NewImage"): Schema(
                        {
                            Required("sk"): Any(str),
                            Required("pk"): Any(str),
                            Required("work_order_number"): Any(str),
                            Required("site_id"): Any(str),
                            Required("sblu"): Any(str),
                            Optional("mod_user"): Any(str),
                            Optional("estimate_item_id"): Any(str),
                            Optional("estimate_labor_id"): Any(str),
                            Optional("estimate_part_id"): Any(str),
                            Optional("hr_updated"): Any(str),
                        },
                        extra=True,
                    ),
                },
                extra=True,
            ),
        },
        extra=True,
    )

    return schema


def get_recon_retail_estimate():
    schema = Schema(
        {
            Required("eventName"): Any("INSERT", "MODIFY", "REMOVE"),
            Required("dynamodb"): Schema(
                {
                    Optional("NewImage"): Schema(
                        {
                            Required("sk"): Any(str),
                            Required("pk"): Any(str),
                            Required("work_order_number"): Any(str),
                            Required("site_id"): Any(str),
                            Required("sblu"): Any(str),
                            Optional("mod_user"): Any(str),
                            Optional("estimate_item_id"): Any(str),
                            Optional("estimate_labor_id"): Any(str),
                            Optional("estimate_part_id"): Any(str),
                            Optional("hr_updated"): Any(str),
                            Optional("fee_id"): Any(str),
                        },
                        extra=True,
                    ),
                },
                extra=True,
            ),
        },
        extra=True,
    )

    return schema


def pfvehicle_validator():
    return Schema(
        {
            Required("work_order_key"): Any(str),
            Required("sblu"): Any(str),
            Required("work_order_number"): Any(str),
            Required("site_id"): Any(str),
            Required("pfvehicle"): Schema(
                {
                    Optional("checkedOutDate"): Any(str),
                    Optional("sellerName"): Any(str),
                    Optional("sellerDealerid"): Any(str),
                    Optional("dispositionCode"): Any(str),
                    Optional("sellerGroupCode"): Any(str),
                    Optional("primeCode"): Any(str),
                    Optional("stockedDate"): Any(str),
                    Optional("titleRecievedFromSellerDate"): Any(str),
                    Optional("titleReturnedToSellerDate"): Any(str),
                    Optional("vehSourceType"): Any(str),
                    Optional("vehStatus"): Any(str),
                    Optional("changeStatus"): All(
                        str,  # Validate that the value is a string
                        Length(min=1),  # Validate that the length is at least 1
                    ),
                    Optional("vin1"): Any(str),
                    Optional("vin2"): Any(str),
                    Optional("vin3"): Any(str),
                    Optional("vin4"): Any(str),
                    Optional("vin5"): Any(str),
                    Optional("vin6"): Any(str),
                    Optional("vin7"): Any(str),
                    Optional("vin8"): Any(str),
                    Optional("vin9"): Any(str),
                    Optional("vin10"): Any(str),
                    Optional("vin11"): Any(str),
                    Optional("vin12"): Any(str),
                    Optional("vin_last_6"): Any(str),
                    Optional("previousVin"): Any(str),
                },
                extra=REMOVE_EXTRA,
            ),
        },
        extra=REMOVE_EXTRA,
    )


def pfvehicle_body_validator():
    return Schema(
        {
            Required("work_order_key"): Any(str),
            Required("sblu"): Any(str),
            Required("work_order_number"): Any(str),
            Required("site_id"): Any(str),
        },
        extra=True,
    )


def pfvehicle_offering_validator():
    return Schema(
        {
            Required("work_order_key"): Any(str),
            Required("sblu"): Any(str),
            Required("work_order_number"): Any(str),
            Required("site_id"): Any(str),
            Required("pfvehicle"): Schema(
                {
                    Optional("buyerNet"): Any(str),
                    Optional("alternateGroupCode"): Any(str),
                    Optional("announcements"): Any(str),
                    Optional("arbInitDate"): Any(str),
                    Optional("otherFee"): Any(str),
                    Optional("buyerFee"): Any(str),
                    Optional("buyerName"): Any(str),
                    Optional("buyerDealerid"): Any(str),
                    Optional("buyeRepId"): Any(str),
                    Optional("buyerUniversal"): Any(str),
                    Optional("ifbid"): Any(str),
                    Optional("sellerDealerid"): Any(str),
                    Optional("sellerGroupCode"): Any(str),
                    Optional("sellerName"): Any(str),
                },
                extra=REMOVE_EXTRA,
            ),
        },
        extra=REMOVE_EXTRA,
    )


def pfvcfn_validator():
    return Schema(
        {
            Required("pfvcfnid"): Any(str),
            Required("sblu"): Any(str),
            Required("work_order_number"): Any(str),
            Required("site_id"): Any(str),
            Required("change_status"): Any(str),
            Required("pfvcfn"): Schema(
                {
                    Optional("VCFNotes1"): Any(str),
                    Optional("VCFNotes2"): Any(str),
                    Required("categoryShortName"): Any(str),
                    Optional("currentStatus"): Any(str),
                    Optional("flagHours"): Any(str),
                    Optional("hours"): Any(str),
                    Optional("panelText"): Any(str),
                    Optional("period"): Any(str),
                    Optional("ratingCode"): Any(str),
                    Optional("recordCode"): Any(str),
                    Optional("sequence"): Any(str),
                    Optional("statusDate"): Any(str),
                    Optional("statusHoursOverride"): Any(str),
                    Optional("teamId"): Any(str),
                },
                extra=REMOVE_EXTRA,
            ),
        },
        extra=REMOVE_EXTRA,
    )


def pfvcflog_validator():
    return Schema(
        {
            Required("pfvcflogid"): Any(str),
            Required("sblu"): Any(str),
            Required("work_order_key"): Any(str),
            Required("site_id"): Any(str),
            Required("pfvcflog"): Schema(
                {
                    Optional("vlnote"): Any(str),
                    Optional("vlnote2"): Any(str),
                    Optional("vlstat"): Any(str),
                    Optional("vlflhr"): Any(str),
                    Optional("vlethr"): Any(str),
                    Optional("vltxt"): Any(str),
                    Optional("vlfcde"): Any(str),
                    Optional("vlteam"): Any(str),
                    Optional("vlstatp"): Any(str),
                    Optional("vlusid"): Any(str),
                    Optional("vlpgm"): Any(str),
                    Optional("vlfeecd"): Any(str),
                    Optional("vleflag"): Any(str),
                    Optional("vlcflag"): Any(str),
                    Optional("vlvcde"): Any(str),
                    Required("id"): Schema(
                        {
                            Required("vldluni"): Any(str),
                            Required("vlfabr"): Any(str),
                            Required("vlseq"): Any(str),
                            Required("vlshdt"): Any(str),
                            Required("vltime"): Any(str),
                        },
                        extra=REMOVE_EXTRA,
                    ),
                },
                extra=REMOVE_EXTRA,
            ),
        },
        extra=REMOVE_EXTRA,
    )


def pfrecon_validator():
    return Schema(
        {
            Required("work_order_key"): Any(str),
            Required("site_id"): Any(str),
            Required("change_status"): Any(str),
            Required("record_sub_menu"): Any(str),
            Required("record_number"): Any(str),
            Required("work_order_number"): Any(str),
            Required("pfrecon"): Schema(
                {
                    Optional("cost"): Any(str),
                    Optional("dateCollected"): Any(str),
                    Optional("dateEntered"): Any(str),
                    Optional("description"): Any(str),
                    Optional("expenseSubClass"): Any(str),
                    Optional("hoursLabor"): Any(str),
                    Optional("laborBillingRate"): Any(str),
                    Optional("laborRate"): Any(str),
                    Optional("partNumber"): Any(str),
                    Optional("quantity"): Any(str),
                    Optional("reconSubMenu"): Any(str),
                    Optional("recordNumber"): Any(str),
                    Optional("retail"): Any(str),
                    Optional("subletName"): Any(str),
                    Optional("vendorNumber"): Any(str),
                },
                extra=REMOVE_EXTRA,
            ),
        },
        extra=REMOVE_EXTRA,
    )


def get_order_retail_recon_estimate():
    schema = Schema(
        {
            Required("eventName"): Any("INSERT", "MODIFY", "REMOVE"),
            Required("dynamodb"): Schema(
                {
                    Optional("NewImage"): Schema(
                        {
                            Required("site_id"): Any(str),
                            Required("sblu"): Any(str),
                            Required("order"): Any(dict),
                        },
                        extra=True,
                    ),
                },
                extra=True,
            ),
        },
        extra=True,
    )
    return schema


def get_offering():
    schema = Schema(
        {
            Required("work_order_key"): Any(str),
        },
        extra=ALLOW_EXTRA,
    )

    return schema


def get_recon_approval():
    schema = Schema(
        {
            Required("status"): Any(str),
            Required("approved_by"): Any(str),
            Optional("notes"): Any(str, None),
            Required("work_order_number"): Any(str),
            Required("work_order_key"): Any(str),
            Required("vin"): Any(str),
            Required("auction_id"): Any(str),
            Required("sblu"): Any(str),
            Required("manheim_account_number"): Any(str),
            Required("source_system"): Any(str),
            Required("updated"): Any(str),
            Required("updated_hr"): Any(str),
        },
        extra=REMOVE_EXTRA,
    )

    return schema


def get_recon_approval_item():
    schema = Schema(
        {
            Required("status"): Any(str),
            Required("approved_by"): Any(str),
            Optional("notes"): Any(str, None),
            Required("item_type"): Any(str),
            Required("item_reference_id"): Any(str),
            Required("work_order_number"): Any(str),
            Required("work_order_key"): Any(str),
            Required("vin"): Any(str),
            Required("auction_id"): Any(str),
            Required("sblu"): Any(str),
            Required("manheim_account_number"): Any(str),
            Required("source_system"): Any(str),
            Required("updated"): Any(str),
            Required("updated_hr"): Any(str),
        },
        extra=REMOVE_EXTRA,
    )

    return schema


def get_rpp_notes():
    schema = Schema(
        {
            Required("eventName"): Any("INSERT"),
            Required("dynamodb"): Schema(
                {
                    Optional("NewImage"): Schema(
                        {
                            Required("pk"): Any(str),
                            Required("sk"): Any(str),
                            Required("work_order_number"): Any(str),
                            Required("site_id"): Any(str),
                            Required("mod_user"): Any(str),
                            Required("vin"): Any(str),
                            Required("notes"): Any(str),
                        },
                        extra=REMOVE_EXTRA,
                    ),
                },
                extra=REMOVE_EXTRA,
            ),
        },
        extra=REMOVE_EXTRA,
    )

    return schema


def get_order_image():
    schema = Schema(
        {
            Required("work_order_key"): Any(str),
            Required("vin"): Any(str),
            Required("sblu"): Any(str),
            Required("work_order_number"): Any(str),
            Required("site_id"): Any(str),
            Required("order"): Schema(
                {
                    Required("images"): Schema(
                        {
                            Required("images"): All(list, Length(min=1))
                        },
                        extra=REMOVE_EXTRA,
                    ),
                },
                extra=REMOVE_EXTRA,
            ),
        },
        extra=REMOVE_EXTRA,
    )
    return schema


def valid_certification_updated(event):
    validator = get_certification_updated()
    return validator(event)


def valid_certification_completed(event):
    validator = get_certification_completed()
    return validator(event)


def valid_certification_canceled(event):
    validator = get_certification_canceled()
    return validator(event)


def valid_condition_updated(event):
    validator = get_condition_updated()
    return validator(event)


def valid_condition_completed(event):
    validator = get_condition_completed()
    return validator(event)


def valid_damage(event):
    validator = get_damage()
    return validator(event)


def valid_damage_labor(event, labor_type):
    validator = get_damage_labor(labor_type)
    return validator(event)


def valid_detail_requested(event):
    validator = get_detail_requested()
    return validator(event)


def valid_detail_completed(event):
    validator = get_detail_completed()
    return validator(event)


def valid_detail_canceled(event):
    validator = get_detail_canceled()
    return validator(event)


def valid_detail_declined(event):
    validator = get_detail_declined()
    return validator(event)


def valid_labor_fee_status(event):
    validator = get_labor_fee_status()
    return validator(event)


def valid_labor_condition_status(event):
    validator = get_labor_condition_status()
    return validator(event)


def valid_offering_sold_updated(event):
    validator = get_offering_sold_updated()
    return validator(event)


def valid_offering_in_lane_updated(event):
    validator = get_offering_in_lane_updated()
    return validator(event)


def valid_offering_canceled(event):
    validator = get_offering_canceled()
    return validator(event)


def valid_approval(event):
    validator = get_approval()
    return validator(event)


def valid_retailrecon_updated(event):
    validator = get_retailrecon_updated()
    return validator(event)


def valid_retailrecon_completed(event):
    validator = get_retailrecon_completed()
    return validator(event)


def valid_vcf_event_created(event):
    validator = get_vcf_event_created()
    return validator(event)


def valid_vcf_event_completed(event):
    validator = get_vcf_event_completed()
    return validator(event)


def valid_new_image(event):
    validator = get_new_image()
    return validator(event)


def validate_work_order_request(event):
    request_validator = get_work_order_request()
    return request_validator(event)


def valid_labor_category_request(event):
    request_validator = get_labor_category()
    return request_validator(event)


def valid_work_credit_condition(event):
    validator = get_work_credit_condition()
    return validator(event)


def valid_work_credit_fee(event):
    validator = get_work_credit_fee()
    return validator(event)


def valid_work_credit(event):
    validator = get_work_credit()
    return validator(event)


def valid_capture(event):
    validator = get_capture()
    return validator(event)


def validate_order_condition_schema(event):
    validator = get_order_condition_schema()
    return validator(event)


def valid_vcf_ad_hoc_created(event, AD_HOC_TUPLE):
    validator = get_vcf_ad_hoc_created(AD_HOC_TUPLE)
    return validator(event)


def valid_vcf_ad_hoc_updated(event):
    validator = get_vcf_ad_hoc_updated()
    return validator(event)


def valid_vcf_ad_hoc_removed(event):
    validator = get_vcf_ad_hoc_removed()
    return validator(event)


def valid_consignment(event):
    validator = get_consignment_event()
    return validator(event)


def valid_retail_inspection(event):
    validator = get_retail_inspection()
    return validator(event)


def valid_retail_estimate(event):
    validator = get_retail_estimate()
    return validator(event)


def valid_recon_retail_estimate(event):
    validator = get_recon_retail_estimate()
    return validator(event)


def validate_pfrecon(event):
    validator = pfrecon_validator()
    return validator(event)


def validate_pfvcflog(event):
    validator = pfvcflog_validator()
    return validator(event)


def validate_pfvcfn(event):
    validator = pfvcfn_validator()
    return validator(event)


def validate_pfvehicle(event):
    validator = pfvehicle_validator()
    return validator(event)


def validate_pfvehicle_body(event):
    validator = pfvehicle_body_validator()
    return validator(event)


def validate_pfvehicle_offering(event):
    validator = pfvehicle_offering_validator()
    return validator(event)


def valid_offering(event):
    validator = get_offering()
    return validator(event)


def valid_order_retail_recon_estimate(event):
    validator = get_order_retail_recon_estimate()
    return validator(event)


def valid_rejection(event):
    validator = get_rejection()
    return validator(event)


def valid_recon_approval(event):
    validator = get_recon_approval()
    return validator(event)


def valid_recon_approval_item(event):
    validator = get_recon_approval_item()
    return validator(event)


def valid_rpp_notes_item(event):
    validator = get_rpp_notes()
    return validator(event)


def valid_order_image(event):
    validator = get_order_image()
    return validator(event)
