
from decimal import Decimal
from time import time

import boto3
from rpp_lib.rpc import get_approval, get_labor_status
from voluptuous import Any
from voluptuous.error import MultipleInvalid
from validator.recon_work_order import validate_process_labor_status, valid_damage_labor
from environs import Env
from botocore.exceptions import ClientError
from rpp_lib.logs import LOGGER
from recon_work_order import find as get_work_order
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()

ENV = Env()
LABOR_TYPES = ("REPAIR", "PAINT", "PART")
DYNAMO = boto3.resource("dynamodb")
WORK_ORDER_TABLE_NAME = ENV("WORKORDER_AM_TABLE", validate=Any(str))
WO_TABLE = DYNAMO.Table(WORK_ORDER_TABLE_NAME)


@xray_recorder.capture("process_labor_status")
def process_labor_status(record):
    try:
        LOGGER.debug({"event": record})
        labor_status = validate_process_labor_status(record)
        work_order_key = labor_status.pop("pk")
        labor_status_sk = labor_status.pop("sk")
        item_code, sub_item_code, damage_code, labor_type = labor_status_sk.split("#")
        action_code = labor_status.pop("action_code", None)
        severity_code = labor_status.pop("severity_code", None)
        if not severity_code:
            approval_info = get_action_severity_code(work_order_key, item_code, sub_item_code, damage_code)
            severity_code = approval_info.get("severityCode", "")
            action_code = approval_info.get("actionCode", "")

        idsa = f"damage:{item_code}"
        isdsa = f"damage:{item_code}#{sub_item_code}"

        if damage_code:
            idsa += f"#{damage_code}"
            isdsa += f"#{damage_code}"

        if severity_code:
            idsa += f"#{severity_code}"
            isdsa += f"#{severity_code}"

        if action_code:
            idsa += f"#{action_code}"
            isdsa += f"#{action_code}"

        isdsa_record = get_work_order({
            "key": {
                "pk": f"workorder:{work_order_key}",
                "sk": isdsa
            }
        }, None)
        idsa_record = get_work_order({
            "key": {
                "pk": f"workorder:{work_order_key}",
                "sk": idsa
            }
        }, None)

        if isdsa_record:
            sk = isdsa
            damage_record = isdsa_record
        elif idsa_record and idsa_record["sub_item_code"] == sub_item_code:
            sk = idsa
            damage_record = idsa_record
        else:
            sk = isdsa
            damage_record = isdsa_record

        LOGGER.debug({
            "labor_status": labor_status,
            "damage_sk": sk,
            "damage_record": damage_record
        })

        xray_recorder.put_metadata("labor_status_record", record)
        xray_recorder.begin_subsegment('labor_status_record')
        xray_recorder.put_annotation("work_order_key", work_order_key)
        if labor_status.get("work_order_number"):
            xray_recorder.put_annotation("work_order_number", labor_status.get("work_order_number"))
        if labor_status.get("sblu"):
            xray_recorder.put_annotation("sblu", labor_status.get("sblu"))
        if labor_status.get("item_code"):
            xray_recorder.put_annotation("item_code", item_code)
        if labor_status.get("damage_code"):
            xray_recorder.put_annotation("damage_code", damage_code)
        if labor_status.get("severity_code"):
            xray_recorder.put_annotation("severity_code", severity_code)
        if labor_status.get("action_code"):
            xray_recorder.put_annotation("action_code", action_code)
        if labor_status.get("sub_item_code"):
            xray_recorder.put_annotation("sub_item_code", sub_item_code)
        if labor_status.get("damage"):
            xray_recorder.put_annotation("damage", labor_status.get("damage"))
        if labor_status.get("item"):
            xray_recorder.put_annotation("item", labor_status.get("item"))
        if labor_status.get("severity"):
            xray_recorder.put_annotation("severity", labor_status.get("severity"))
        if labor_status.get("action"):
            xray_recorder.put_annotation("action", labor_status.get("action"))
        xray_recorder.end_subsegment()

        if damage_record:
            damage_record.update({
                labor_type.lower() + "_status": labor_status["current_status"]["labor_status"],
                "updated_date": labor_status["current_status"]["date"],
                "updated": Decimal(time()),
                "source": labor_status['current_status']['source'],
                labor_type.lower() + "_updated": labor_status["updated"]
            })
            xray_recorder.put_annotation("source", labor_status['current_status']['source'])
            if labor_status["current_status"].get("updated_by"):
                damage_record.update({"updated_by": labor_status["current_status"]["updated_by"]})
                xray_recorder.put_annotation("updated_by", labor_status['current_status']['updated_by'])

            remove_statuses = []
            if labor_status.get("charge_l_status"):
                damage_record.update({"charge_l_status": labor_status["charge_l_status"]})
            else:
                remove_statuses.append("charge_l_status")
                damage_record.pop("charge_l_status", None)

            if labor_status.get("charge_p_status"):
                damage_record.update({"charge_p_status": labor_status["charge_p_status"]})
            else:
                remove_statuses.append("charge_p_status")
                damage_record.pop("charge_p_status", None)

            if remove_statuses:
                delete_attribute({
                    "pk": f"workorder:{work_order_key}",
                    "sk": sk
                }, remove_statuses)
            damage_record.pop("pk")
            damage_record.pop("sk")
            get_overall_damage_status(damage_record)
            update_work_order(work_order_key, sk, damage_record, labor_type)
        else:
            LOGGER.warning({
                "Message": f"No record found for {sk} combination, skipping update."
            })

    except MultipleInvalid as validation_error:
        message = {
            "message": "Not meeting the validation, skipping",
            "reason": str(validation_error),
            "event": record,
        }
        LOGGER.info(message)

    except ValueError as value_error:
        message = {
            "message": "sk is not isd format, could be ad hoc damage, skipping",
            "reason": str(value_error),
            "event": record,
        }
        LOGGER.info(message)

    except ClientError as c_err:
        error_code = c_err.response["Error"]["Code"]
        LOGGER.info({
            "labor_status": labor_status,
            "error_code": error_code,
            "c_error": c_err
        })


def update_work_order(work_order_key, sk, damage_record, labor_type):
    """
            Update work-order row info for a given workorder
        """

    LOGGER.debug({"workorder": work_order_key, "sk": sk})
    key = {
        "pk": f"workorder:{work_order_key}",
        "sk": sk
    }

    attribute_names = {}
    attribute_values = {}

    update_expression = "set "
    update_expression += ",".join(
        ["#" + k + " = :" + k for k in damage_record.keys()]
    )

    attribute_names.update({"#" + k: k for k in damage_record.keys()})

    attribute_values.update({":" + k: damage_record[k] for k in damage_record.keys()})
    if labor_type == "REPAIR":
        condition_expression = "attribute_not_exists(#repair_updated) OR #repair_updated <= :updated"
    if labor_type == "PART":
        condition_expression = "attribute_not_exists(#part_updated) OR #part_updated <= :updated"
    if labor_type == "PAINT":
        condition_expression = "attribute_not_exists(#paint_updated) OR #paint_updated <= :updated"

    LOGGER.info({
        "key": key,
        "update_expression": update_expression,
        "condition_expression": condition_expression,
        "attribute_names": attribute_names,
        "attribute_values": attribute_values
    })

    response = WO_TABLE.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ConditionExpression=condition_expression,
        ExpressionAttributeNames=attribute_names,
        ExpressionAttributeValues=attribute_values,
        ReturnValues="UPDATED_NEW",
    )

    return response['Attributes']


def delete_attribute(key, attributes):
    update_expression = "REMOVE "
    update_expression += ", ".join(attributes)

    LOGGER.info(
        {
            "message": "Removing attribute from record",
            "key": key,
            "update_expression": update_expression,
        }
    )

    response = WO_TABLE.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ReturnValues="UPDATED_NEW",
    )

    LOGGER.debug({"response": response})

    return response


def get_action_severity_code(work_order_key, item_code, sub_item_code, damage_code):
    """Identifies the action code/severity code of the damage from order approval record.

    Returns:
        A dict contain actionCode and severityCode from rpp-order-approval
    """
    approval_response = get_approval(work_order_key=work_order_key)
    if approval_response:
        approval = approval_response[0]
        LOGGER.debug({"approval response": approval})
        damage = next(
            (
                dmg
                for dmg in approval.get("order", {})
                .get("condition", {})
                .get("damages", [])
                if dmg["damageCode"] == damage_code
                and dmg["itemCode"] == item_code
                and dmg["subItemCode"] == sub_item_code
            ),
            {},
        )
        return {k: damage[k] for k in ["actionCode", "severityCode"] if k in damage}
    return {"severityCode": "", "actionCode": ""}


def update_labor_status(labor, work_order_key, sk, damage):
    """
    Function to update damage information with recon-labor-status information
    """

    labor_status = get_labor_status(work_order_key, sk)
    LOGGER.debug({"labor_status": labor_status})
    labor_type = labor.get("damage_labor_type")
    try:
        if labor_status:
            if labor_status.get("current_status"):
                damage.update({
                    labor_type.lower() + "_status": labor_status["current_status"]["labor_status"],
                    "updated_date": labor_status["current_status"]["date"],
                    "source": labor_status['current_status']['source'],
                    "is_estimate_assistant": "N"
                })
                if labor_status["current_status"].get("updated_by"):
                    damage.update({"updated_by": labor_status["current_status"]["updated_by"]})

            remove_statuses = []

            if labor_status.get("charge_l_status"):
                damage.update({"charge_l_status": labor_status["charge_l_status"]})
            else:
                remove_statuses.append("charge_l_status")
            if labor_status.get("charge_p_status"):
                damage.update({"charge_p_status": labor_status["charge_p_status"]})
            else:
                remove_statuses.append("charge_p_status")
            if remove_statuses:
                delete_attribute({
                    "pk": f"workorder:{work_order_key}",
                    "sk": sk
                }, remove_statuses)
        else:
            damage.update({
                labor_type + "_status": "READY FOR REPAIR",
                "source": "approval event",
                "is_estimate_assistant": "N"
            })
    except (KeyError, MultipleInvalid) as error:
        LOGGER.warning(
            {
                "message": "Unable to add work credit to labor.",
                "work_order_key": work_order_key,
                "labor_status": labor_status,
                "error": str(error),
            }
        )

    return damage


def build_labor(damage, labor_type):
    """Returns labor based on labor_type."""
    labor = dict.copy(damage)
    try:
        labor = valid_damage_labor(dict(labor.items()), labor_type.lower())
        labor.update(
            {
                "damage_labor_type": labor_type.lower(),
                "labor_cost": labor[labor_type.lower() + "_labor_cost"],
                "labor_hours": labor[labor_type.lower() + "_labor_hours"],
                "name": ("Paint " + labor["item"])
                if labor_type == "PAINT"
                else (labor["action"] + " " + labor["item"]),
                "updated": Decimal(time()),
            }
        )

        if labor["labor_hours"] == 0:
            new_labor_hours = labor.get(labor_type.lower() + "_labor_cost", Decimal(0)) / 50
            labor.update(
                {
                    "labor_hours": new_labor_hours,
                    labor["damage_labor_type"].lower() + "_labor_hours": new_labor_hours,
                }
            )
    except MultipleInvalid as valid_error:
        labor.update({"damage_labor_type": labor_type.lower()})
        LOGGER.debug({
            "Error": valid_error,
            "damage": damage
        })

    return labor


def get_overall_damage_status(damage_record):
    """
    Get all status for all existing actions for the damage
    part, repair paint, and if all actions are completed then the status is completed=Y
    """
    statuses = [
        {
            "status": damage_record["".join((labor_type.lower(), "_status"))],
            "labor_type": labor_type.lower(),
            f"{labor_type.lower()}_labor_cost": Decimal(
                damage_record.get("".join((labor_type.lower(), "_labor_cost")), 0)
            ),
            f"{labor_type.lower()}_labor_hours": Decimal(
                damage_record.get("".join((labor_type.lower(), "_labor_hours")), 0)
            ),
        }
        for labor_type in LABOR_TYPES
        if damage_record.get("".join((labor_type.lower(), "_status")), "")
    ]

    LOGGER.debug({"Damage_record": damage_record, "statuses": statuses})

    repaired = "Y"
    for status_map in statuses:
        labor_type = status_map["labor_type"]
        if status_map["status"] != "COMPLETED" and (
            status_map[f"{labor_type}_labor_cost"] != 0
            or status_map[f"{labor_type}_labor_hours"] != 0
        ):
            repaired = "N"
            break

    damage_record.update({"repaired": repaired})
