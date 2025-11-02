"""
    order offering functions
"""

import boto3
from botocore.exceptions import ClientError
from environs import Env
from rpp_lib.logs import LOGGER
from voluptuous import Any, MultipleInvalid
from dynamodb.store import put_work_order
import stringcase
from decimal import Decimal
from rpp_lib.validation import validate_unit
import time
from rpp_lib.rpc import get_unit
from validation import (
    valid_offering_canceled,
    valid_offering_in_lane_updated,
    valid_offering_sold_updated,
    valid_offering,
)
import json
from rpp_lib.rpc import get_pfvehicle
from utils.common import get_vin

ENV = Env()
DYNAMO = boto3.resource("dynamodb")
SALE_EVENT_TABLE = DYNAMO.Table(name=ENV("SALE_EVENT_TABLE", validate=Any(str)))
RECON_WORK_ORDER_TABLE = DYNAMO.Table(name=ENV("WORKORDER_AM_TABLE", validate=Any(str)))
GENERIC_SALE_EVENT_GSI = "auctionId-year-saleNumber-computerLane-index"
SALE_DATE_APPROXIMATE_GSI = "auctionId-saleYear-saleNumber-index"

SOLD = "SOLD"


def get_sale_information(
        location_code, sale_year, sale_number, computer_lane, consignor_id
):
    generic_sale_events_key = str(location_code)
    generic_sale_events_key += ":" + str(sale_year)
    generic_sale_events_key += ":" + str(sale_number)
    generic_sale_events_key += ":" + str(computer_lane)

    sale_date = None
    sale_type = None
    sale_date_approximate = None
    min_sale_date = False

    # Attempt 1 to determine sale_date and sale_types
    sale_date, sale_type = query_sale_events_table(
        generic_sale_events_key, GENERIC_SALE_EVENT_GSI, min_sale_date, consignor_id
    )

    if sale_date:
        sale_date_approximate = False

    else:

        # Attempt 2 to determine sale_date and sale_type
        consignor_id = "OPEN"
        sale_date, sale_type = query_sale_events_table(
            generic_sale_events_key, GENERIC_SALE_EVENT_GSI, min_sale_date, consignor_id
        )

        if sale_date:
            sale_date_approximate = False

        else:

            # Attempt 3 to determine sale_date and sale_type
            sale_date, sale_type = query_sale_events_table(
                generic_sale_events_key, GENERIC_SALE_EVENT_GSI, min_sale_date
            )

            if sale_date:
                sale_date_approximate = True

            else:

                # Attempt 4 to determine sale_date and sale_type
                sale_events_approximate_key = str(location_code)
                sale_events_approximate_key += ":" + str(sale_year)
                sale_events_approximate_key += ":" + str(sale_number)

                min_sale_date = True
                sale_date, sale_type = query_sale_events_table(
                    sale_events_approximate_key,
                    SALE_DATE_APPROXIMATE_GSI,
                    min_sale_date,
                )

                if sale_date:
                    sale_date_approximate = True

    return sale_date, sale_type, sale_date_approximate


def get_minimum_sale_start_time(records):
    sale_date_list = [
        record["sale_start_time"] for record in records if "sale_start_time" in record
    ]
    sale_date = min(sale_date_list)

    return sale_date


def query_sale_events_table(key, index, min_sale_date, consignor_id=False):
    sale_date = None
    sale_type = None

    if not min_sale_date and consignor_id:
        key_condition_expression = (
            "#sale_events_key = :sale_events_key AND #consignor_id = :consignor_id"
        )
        expression_attribute_names = {
            "#sale_events_key": "sale_events_key",
            "#consignor_id": "consignor_id",
        }
        expression_attribute_values = {
            ":sale_events_key": key,
            ":consignor_id": consignor_id,
        }
    elif not min_sale_date and not consignor_id:
        key_condition_expression = "#sale_events_key = :sale_events_key"
        expression_attribute_names = {"#sale_events_key": "sale_events_key"}
        expression_attribute_values = {":sale_events_key": key}
    else:
        key_condition_expression = (
            "#sale_events_approximate_key = :sale_events_approximate_key"
        )
        expression_attribute_names = {
            "#sale_events_approximate_key": "sale_events_approximate_key"
        }
        expression_attribute_values = {":sale_events_approximate_key": key}

    LOGGER.debug(
        {
            "gsi": index,
            "key_condition_expression": key_condition_expression,
            "expression_attribute_names": expression_attribute_names,
            "expression_attribute_values": expression_attribute_values,
            "min_sale_date": min_sale_date,
            "consignor_id": consignor_id,
        }
    )

    response = SALE_EVENT_TABLE.query(
        IndexName=index,
        KeyConditionExpression=key_condition_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
    )

    if response["Items"]:

        sale_type = response["Items"][0].get("sale_type")

        if min_sale_date and len(response["Items"]) > 1:
            sale_date = get_minimum_sale_start_time(response["Items"])
        else:
            sale_date = response["Items"][0]["sale_start_time"]

    LOGGER.debug(
        {
            "sale events response": response,
            "sale_date": sale_date,
            "sale_type": sale_type,
        }
    )
    return sale_date, sale_type


# pylint: disable=too-many-arguments, too-many-locals
def store_wo_record(new_image, updated, column, unit, table, new_column=False):
    """
        Store workorder service to dynamodb
    """

    work_order_key = new_image["sblu"]
    work_order_key += "#"
    work_order_key += new_image["site_id"]

    sale_date = None
    sale_type = None
    sale_date_approximate = None

    LOGGER.debug({"store_wo_record(column)": column, "store_wo_record(unit)": unit})

    if column["data"]["action"] == "delete":
        response = table.update_item(
            Key={"work_order_key": work_order_key, "site_id": new_image["site_id"]},
            UpdateExpression="REMOVE #sale_date, #sale_type, #sale_date_approximate",
            ExpressionAttributeNames={
                "#sale_date": "sale_date",
                "#sale_type": "sale_type",
                "#sale_date_approximate": "sale_date_approximate",
            },
            ReturnValues="UPDATED_NEW",
        )
    else:
        offering_status = column.get("data", {}).get("status")

        if offering_status != SOLD:
            sale_date, sale_type, sale_date_approximate = get_sale_information(
                location_code=new_image["site_id"],
                sale_year=column["data"]["saleYear"],
                sale_number=column["data"]["saleNumber"],
                computer_lane=column["data"]["virtualLaneNumber"],
                consignor_id=unit["account"]["groupCode"],
            )

            LOGGER.debug(
                {
                    "work_order_key": work_order_key,
                    "sale_date": sale_date,
                    "sale_type": sale_type,
                    "sale_date_approximate": sale_date_approximate,
                }
            )

    if isinstance(updated, float):
        updated = Decimal(str(updated))

    if isinstance(updated, str):
        updated = Decimal(str(updated))

    key = {"work_order_key": work_order_key, "site_id": new_image["site_id"]}

    update_expression = "set \
    #column_name = :column_data, \
    #column_updated = :updated"

    condition_expression = "#work_order_key = :work_order_key AND \
    (attribute_not_exists(#column_updated) OR  #column_updated < :updated)"

    expression_attribute_names = {
        "#work_order_key": "work_order_key",
        "#column_name": column["name"],
        "#column_updated": column.get("updated_name", column["name"] + "_updated"),
    }

    expression_attribute_values = {
        ":work_order_key": work_order_key,
        ":column_data": column["data"],
        ":updated": updated,
    }

    if sale_date is not None:
        update_expression += ", #sale_date = :sale_date, \
        #initial_sale_date = :initial_sale_date"

        condition_expression += " AND attribute_not_exists(#initial_sale_date)"
        expression_attribute_names.update(
            {"#sale_date": "sale_date", "#initial_sale_date": "initial_sale_date"}
        )
        expression_attribute_values.update(
            {":sale_date": sale_date, ":initial_sale_date": sale_date}
        )

    if sale_type is not None:
        update_expression += ", #sale_type = :sale_type"
        expression_attribute_names.update({"#sale_type": "sale_type"})
        expression_attribute_values.update({":sale_type": sale_type})

    if sale_date_approximate is not None:
        update_expression += ", #sale_date_approximate = :sale_date_approximate"
        expression_attribute_names.update(
            {"#sale_date_approximate": "sale_date_approximate"}
        )
        expression_attribute_values.update(
            {":sale_date_approximate": sale_date_approximate}
        )

    LOGGER.debug(
        {
            "message": "store record via the following",
            "unit": unit,
            "new_column": new_column,
            "update_expression": update_expression,
            "condition_expression": condition_expression,
            "expression_attribute_names": expression_attribute_names,
            "expression_attribute_values": expression_attribute_values,
        }
    )

    response = None
    try:
        response = table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ConditionExpression=condition_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW",
        )
    except ClientError as c_err:
        error_code = c_err.response["Error"]["Code"]
        if error_code == "ConditionalCheckFailedException" and sale_date is not None:

            update_expression = "set \
            #column_name = :column_data, \
            #sale_date = :sale_date, \
            #column_updated = :updated"

            condition_expression = "#work_order_key = :work_order_key AND \
            (attribute_not_exists(#column_updated) OR \
                #column_updated < :updated)"

            expression_attribute_names = {
                "#work_order_key": "work_order_key",
                "#column_name": column["name"],
                "#sale_date": "sale_date",
                "#sale_type": "sale_type",
                "#column_updated": column.get(
                    "updated_name", column["name"] + "_updated"
                ),
            }

            expression_attribute_values = {
                ":work_order_key": work_order_key,
                ":sale_date": sale_date,
                ":sale_type": sale_type,
                ":column_data": column["data"],
                ":updated": updated,
            }

            if sale_type is not None:
                update_expression += ", #sale_type = :sale_type"
                expression_attribute_names.update({"#sale_type": "sale_type"})
                expression_attribute_values.update({":sale_type": sale_type})

            if sale_date_approximate is not None:
                update_expression += ", #sale_date_approximate = :sale_date_approximate"
                expression_attribute_names.update(
                    {"#sale_date_approximate": "sale_date_approximate"}
                )
                expression_attribute_values.update(
                    {":sale_date_approximate": sale_date_approximate}
                )

            response = table.update_item(
                Key=key,
                UpdateExpression=update_expression,
                ConditionExpression=condition_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="UPDATED_NEW",
            )
        else:
            raise c_err

    return response


def build_order(new_record):
    """
        build offering order dictionary
    """

    order = None
    new_record["order"].pop("consignment")
    offering_service = {"offering_id": new_record["offering_id"]}

    try:
        order = valid_offering_canceled(new_record["order"])
        LOGGER.debug("------->  after valid_offering_canceled")
        order.update(offering_service)
        order.update({"action": "delete"})

    except MultipleInvalid as invalid_cancelled_err:
        message = {
            "message": "failed valid_offering_canceled",
            "reason": str(invalid_cancelled_err),
            "work_order_key": new_record["work_order_key"],
        }
        LOGGER.warning(message)

        try:
            order = valid_offering_sold_updated(new_record["order"])
            order.update(offering_service)
            order.update({"action": "update"})

        except MultipleInvalid as invalid_sold_err:
            message = {
                "message": "failed valid_offering_sold",
                "reason": str(invalid_sold_err),
                "work_order_key": new_record["work_order_key"],
            }
            LOGGER.warning(message)

            try:
                order = valid_offering_in_lane_updated(new_record["order"])
                order.update(offering_service)
                order.update({"action": "update"})

            except MultipleInvalid as validation_error:
                message = {
                    "message": "Ignoring offering event",
                    "reason": str(validation_error),
                    "event": new_record,
                }
                LOGGER.warning(message)

    return order


def get_order_offering(new_image):
    """
        return order offering column
    """

    column = None
    try:
        data = build_order(new_image)
        if data is not None:
            column = {
                "name": "offering",
                "store_wo_record": store_wo_record,
                "data": data,
            }
    except KeyError as k_error:
        message = {
            "message": "Bad offering event",
            "reason": str(k_error),
            "event": new_image,
        }
        LOGGER.warning(message)

    return column


def lookup_unit(new_image):
    """
    call rpp_lib.rpc.get_unit to retrieve unit for this WO
    """
    unit_id = new_image["consignment"]["unit"]["href"].rsplit("/", 1)[-1]

    LOGGER.debug({"unit_id": unit_id})

    unit = validate_unit(get_unit(unit_id))
    if "errorMessage" in unit:
        error_response = {
            "Error": {"Code": unit["errorType"], "Message": unit["errorMessage"]}
        }
        c_err = ClientError(error_response, "remote:FIND_UNIT")
        raise c_err
    return unit


def add_offering_record(new_image, order, document_name):
    LOGGER.debug(
        {
            "Adding offering event to dynamo": new_image,
            "order": order
        }
    )

    wo_key = new_image['work_order_key']

    sblu = new_image.get("sblu", None)
    vin = new_image.get("vin", None)
    site_id = new_image.get("site_id", None)
    work_order_number = new_image.get("work_order_number", None)

    if not sblu or not vin or not site_id or not work_order_number:
        pfvehicle = json.loads(get_pfvehicle(work_order_key=wo_key))

        if not sblu and pfvehicle:
            sblu = pfvehicle["sblu"]
        if not vin and pfvehicle:
            pfvehicle_vin = get_vin(pfvehicle("pfvehicle"))
            if pfvehicle_vin:
                vin = pfvehicle_vin
        if not site_id and pfvehicle:
            site_id = pfvehicle["site_id"]
        if not work_order_number and pfvehicle:
            work_order_number = pfvehicle["work_order_number"]

    record_data = {
        "sblu": sblu,
        "site_id": site_id,
        "work_order_number": work_order_number,
        "vin": vin,
        "updated": Decimal(time.time()),
        "key_src": "offering_id",
        "entity_type": "offering"
    }
    record_data.update({
        stringcase.snakecase(k): v for k, v in
        order.items()
    })
    try:
        put_work_order(wo_key, document_name, record_data)
    except ClientError as c_err:
        raise c_err


def update_summary_record(wo_key, new_image, unit, action):
    Key = {"pk": wo_key, "sk": wo_key}

    LOGGER.debug(
        {
            "Adding sale info": new_image,
            "Key": Key,
            "action": action
        }
    )
    # moved below code to consignment.
    # if unit["contact"]["companyName"]:
    #     RECON_WORK_ORDER_TABLE.update_item(
    #         Key=Key,
    #         UpdateExpression="set #company_name = :company_name",
    #         ExpressionAttributeNames={
    #             "#company_name": "company_name",
    #         },
    #         ExpressionAttributeValues={
    #             ":company_name": unit["contact"]["companyName"]
    #         },
    #         ReturnValues="UPDATED_NEW",
    #     )

    if action == "delete":
        RECON_WORK_ORDER_TABLE.update_item(
            Key=Key,
            UpdateExpression="REMOVE #sale_date, #sale_type, #sale_date_approximate",
            ExpressionAttributeNames={
                "#sale_date": "sale_date",
                "#sale_type": "sale_type",
                "#sale_date_approximate": "sale_date_approximate",
            },
            ReturnValues="UPDATED_NEW",
        )

    elif action == "update":
        sale_date = None
        sale_type = None
        sale_date_approximate = None

        if new_image["order"]["status"] != 'SOLD':
            sale_date, sale_type, sale_date_approximate = get_sale_information(
                location_code=new_image["site_id"],
                sale_year=new_image["order"]["saleYear"],
                sale_number=new_image["order"]["saleNumber"],
                computer_lane=new_image["order"]["virtualLaneNumber"],
                consignor_id=unit["account"]["groupCode"],
            )
            LOGGER.debug(
                {
                    "work_order_key": wo_key,
                    "sale_date": sale_date,
                    "sale_type": sale_type,
                    "sale_date_approximate": sale_date_approximate,
                }
            )
        update_expression = "set "
        condition_expression = ""
        expression_attribute_names = {}
        expression_attribute_values = {}

        if sale_date is not None:
            update_expression += "#sale_date = :sale_date, \
               #initial_sale_date = :initial_sale_date"
            condition_expression += "attribute_not_exists(#initial_sale_date)"
            expression_attribute_names.update(
                {"#sale_date": "sale_date", "#initial_sale_date": "initial_sale_date"}
            )
            expression_attribute_values.update(
                {":sale_date": sale_date, ":initial_sale_date": sale_date}
            )

        if sale_type is not None:
            update_expression += ", #sale_type = :sale_type"
            expression_attribute_names.update({"#sale_type": "sale_type"})
            expression_attribute_values.update({":sale_type": sale_type})

        if sale_date_approximate is not None:
            update_expression += ", #sale_date_approximate = :sale_date_approximate"
            expression_attribute_names.update(
                {"#sale_date_approximate": "sale_date_approximate"}
            )
            expression_attribute_values.update(
                {":sale_date_approximate": sale_date_approximate}
            )

            LOGGER.debug(
                {
                    "message": "updating dynnamo",
                    "updat_expression": update_expression,
                    "condition_expression": condition_expression,
                    "attribute_names": expression_attribute_names,
                    "attribute_values": expression_attribute_values
                }
            )
        if expression_attribute_names:
            try:
                RECON_WORK_ORDER_TABLE.update_item(
                    Key=Key,
                    UpdateExpression=update_expression,
                    ConditionExpression=condition_expression,
                    ExpressionAttributeNames=expression_attribute_names,
                    ExpressionAttributeValues=expression_attribute_values,
                    ReturnValues="UPDATED_NEW",
                )
            except ClientError as c_err:
                error_code = c_err.response["Error"]["Code"]
                if error_code == "ConditionalCheckFailedException" and sale_date is not None:

                    update_expression = "set \
                    #sale_date = :sale_date"

                    expression_attribute_names = {
                        "#sale_date": "sale_date"
                    }
                    expression_attribute_values = {
                        ":sale_date": sale_date
                    }

                    if sale_type is not None:
                        update_expression += ", #sale_type = :sale_type"
                        expression_attribute_names.update({"#sale_type": "sale_type"})
                        expression_attribute_values.update({":sale_type": sale_type})

                    if sale_date_approximate is not None:
                        update_expression += ", #sale_date_approximate = :sale_date_approximate"
                        expression_attribute_names.update(
                            {"#sale_date_approximate": "sale_date_approximate"}
                        )
                        expression_attribute_values.update(
                            {":sale_date_approximate": sale_date_approximate}
                        )

                    LOGGER.debug(
                        {
                            "message": "updating dynnamo",
                            "updat_expression": update_expression,
                            "attribute_names": expression_attribute_names,
                            "attribute_values": expression_attribute_values
                        }
                    )

                    RECON_WORK_ORDER_TABLE.update_item(
                        Key=Key,
                        UpdateExpression=update_expression,
                        ExpressionAttributeNames=expression_attribute_names,
                        ExpressionAttributeValues=expression_attribute_values,
                        ReturnValues="UPDATED_NEW",
                    )
                else:
                    raise c_err


def add_offering_data(new_image, document_name):
    '''
    Function that will add offering information to the given
    record matching the document_name.
    '''
    LOGGER.debug({
        "processing offering event": new_image,
        "document": document_name
    })
    record = valid_offering(new_image)
    order = build_order(new_image)
    if order:
        # Adding offering event into table.
        add_offering_record(new_image, order, document_name)

        wo_key = record['work_order_key']
        unit = lookup_unit(new_image)

        # Adding sale info into summary
        update_summary_record(f"workorder:{wo_key}", new_image, unit, order["action"])
