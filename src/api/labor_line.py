"""
Record laborline attribute to workorder table
"""
import json
import logging
import os

# pylint: disable=unused-import
from aws_xray_sdk.core import xray_recorder  # noqa: F401
from aws_xray_sdk.core import patch_all

from utils.dynamodb import get_item, query, update

patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def labor_line_handler(event, _):
    """
    Hanldler of labor line
    """
    records = event["Records"]

    if not records or records is False:
        logger.info("Records is empty")
        return False

    sns = records[0]["Sns"]
    if not sns:
        logger.info("Sns is empty")
        return False
    message = json.loads(sns["Message"])

    res = save_labor_line(message)
    return res


def build_labor_line_object(message, flatFeeTableData):
    """
    build labor line object
    """
    if message["id"]["vlfabr"] != "UCFIN" or message["vlstat"] != "UI":
        logger.info(
            "%s is %s UCFIN",
            message["id"]["vlfabr"],
            "equal to" if message["id"]["vlfabr"] == "UCFIN" else "not equal to",
        )
        logger.info(
            "%s is %s UI",
            message["vlstat"],
            "equal to" if message["vlstat"] == "UI" else "not equal to",
        )
        return False
    found = False
    hours = None
    lName = None
    flatFeeValues = flatFeeTableData["Item"]["vcf_categories"]
    for item in flatFeeValues:
        if item["category"] == message["id"]["vlfabr"]:
            hours = item["hours"]
            lName = item["laborName"]
            found = True
            break
    if not found:
        logger.info("Item not found for %s", message["id"]["vlfabr"])
        return False
    laborLine = {
        "ucfin": {
            "shop": "UCFIN",
            "labor_hours": str(hours),
            "labor_name": lName,
            "labor_status": "Ready for Repair",
        }
    }
    return laborLine


def save_labor_line(vcfData):
    """
    save labor line object to workorder table
    """
    sblu = vcfData["id"]["vlsblu"]
    site_id = vcfData["id"]["vlauci"]
    try:
        workOrderData = query(
            os.getenv("WORKORDER_TABLE"),
            "sblu",
            sblu,
            "site_id",
            site_id,
            "sblu, site_id",
        )
    except Exception:
        logger.info(
            "An error occur to get info from workorder table, paramaters sblu = %s , site_id = %s",
            sblu,
            site_id,
        )
        return False
    if workOrderData["Count"] != 0:
        try:
            partition_key = "key"
            partition_value = "vcfCategories"
            flatFeeData = get_item(
                os.getenv("FLAT_FEE_TABLE"), partition_key, partition_value
            )
        except Exception:
            logger.info(
                "An error occur to get info from flatfee table, partition key = %s and value = %s",
                partition_key,
                partition_value,
            )
            return False

        laborLineObject = build_labor_line_object(vcfData, flatFeeData)

        if laborLineObject is False:
            logger.info(
                "There was an error to transform data to create the laborline object"
            )
            return False

        try:
            response = update(
                os.getenv("WORKORDER_TABLE"),
                {"sblu": sblu, "site_id": site_id},
                laborLineObject,
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logger.info("Labor Line added succesfully to the workorder table")
                return True
            logger.info(
                "There was an error to add Labor Line Object into workorder table"
            )
            return False
        except Exception:
            logger.info(
                '%s, key = {"sblu": %s, "site_id": %s} and labor line object = %s',
                "There was an error to record into workorder table",
                sblu,
                site_id,
                laborLineObject,
            )
            return False
    else:
        logger.info("There is no information in workorder table.")
        return False
