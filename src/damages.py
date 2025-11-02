"""
    approved damages based on order approval
"""
from datetime import datetime
from decimal import Decimal
from time import time

from rpp_lib.logs import LOGGER
from rpp_lib.rpc import get_labor_status, get_work_credit
from stringcase import snakecase
from voluptuous import MultipleInvalid

from validation import valid_damage, valid_damage_labor, valid_work_credit

LABOR_TYPES = ("REPAIR", "PAINT", "PART")


def build_damages(order, work_order_key):
    """
        build damages dictionary
    """
    dmg_map = None
    try:
        condition = order["condition"]
        damages = condition["damages"]
        dmg_map, declined_dmg_list = create_damage_list(damages, work_order_key)

        LOGGER.debug(
            {"Damages to add": dmg_map, "Damages not included": declined_dmg_list}
        )

    except MultipleInvalid as validation_error:
        message = {
            "message": "Ignoring damage to create damage map",
            "reason": str(validation_error),
            "event": order,
        }

        LOGGER.warning(message)

    return dmg_map


def create_damage_list(damages, work_order_key):
    """
    creates a damage list of maps based on approved damages using ISDT as key
    """
    approved_damages = {}
    declined_damages = []
    for damage in damages:
        try:
            damage = valid_damage(damage)
            dmg = {snakecase(k): v for (k, v) in damage.items()}
            labors = [
                build_labor(dmg, labor_type)
                for labor_type in LABOR_TYPES
                if dmg.get(labor_type.lower() + "_labor_hours") != 0
                or dmg.get(labor_type.lower() + "_labor_cost") != 0
            ]

            if not labors:
                labors.append(build_labor(dmg, "REPAIR"))

            for labor in labors:
                isdt_key = create_isdt_key(labor)
                labor = update_labor(labor, isdt_key, work_order_key)
                approved_damages.update({isdt_key: labor})

        except MultipleInvalid as valid_error:
            LOGGER.debug({"Validation_error": valid_error})
            declined_damages.append(damage)

    return approved_damages, declined_damages


def build_labor(damage, labor_type):
    """Returns labor based on labor_type."""
    labor = valid_damage_labor(dict(damage.items()), labor_type.lower())
    labor.update(
        {
            "damage_labor_type": labor_type.lower(),
            "labor_cost": labor[labor_type.lower() + "_labor_cost"],
            "labor_hours": labor[labor_type.lower() + "_labor_hours"],
            "name": ("Paint " + labor["item"])
            if labor_type == "PAINT"
            else (labor["action"] + " " + labor["item"]),
            "updated": Decimal(round(time(), 3)),
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

    return labor


def update_labor(labor, isdt_key, work_order_key):
    """Logic to logic to upate the labor with latest status and work credit data in PE Engine"""

    if "repair_completion_date" in labor:
        labor.update(
            {
                "current_status": {
                    "date": datetime.strptime(
                        labor["repair_completion_date"], "%Y-%m-%d"
                    ).isoformat(),
                    "source": "Condition API",
                    "labor_status": "COMPLETED",
                    "updated_by": "Condition API",
                }
            }
        )

    labor_status = get_labor_status(work_order_key, isdt_key)

    if labor_status:
        labor.update({"current_status": labor_status["current_status"]})

        if labor_status.get("charge_l_status"):
            labor.update({"charge_l_status": labor_status["charge_l_status"]})

        if labor_status.get("charge_p_status"):
            labor.update({"charge_p_status": labor_status["charge_p_status"]})

    try:
        work_credit_info = get_work_credit(work_order_key, isdt_key)
        work_credit = valid_work_credit(work_credit_info)

        if work_credit:
            labor.update(work_credit)

    except (KeyError, MultipleInvalid) as error:
        LOGGER.warning(
            {
                "message": "Unable to add work credit to labor.",
                "work_order_key": work_order_key,
                "work_credit": work_credit_info,
                "error": str(error),
            }
        )

    return labor


def create_isdt_key(damage):
    """ Create the ISDT key using item_code, sub_item_code, damage_code  and damage_labor_type"""
    isdt = damage["item_code"]
    isdt += "#"
    isdt += damage["sub_item_code"]
    isdt += "#"
    isdt += damage["damage_code"]
    isdt += "#"
    isdt += damage["damage_labor_type"]
    return isdt.upper()
