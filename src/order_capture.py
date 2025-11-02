from decimal import Decimal
import time
from dynamodb.store import put_work_order
import stringcase
from validation import valid_capture
from rpp_lib.rpc import get_pfvehicle
from utils.common import get_vin
import json


def add_capture_data(new_image, document_name):
    '''
    Function that will add order information to the given
    record matching the document_name.
    '''

    record = valid_capture(new_image)

    wo_key = record['work_order_key']

    sblu = record.get("sblu", None)
    vin = record.get("vin", None)
    site_id = record.get("site_id", None)
    work_order_number = record.get("work_order_number", None)

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

    order_data = {}
    order_data.update({
        stringcase.snakecase(k): v for k, v in
        record["order"].items()
    })
    record_data = {
        'capture_status': order_data.get('status'),
        'capture_completed_timestamp': order_data.get('completed_timestamp'),
        "capture_updated": Decimal(time.time()),
        "sblu": sblu,
        "entity_type": document_name,
        "vin": vin,
        "site_id": site_id,
        "work_order_number": work_order_number,
        "updated": Decimal(time.time())
    }

    put_work_order(wo_key, document_name, record_data)


def add_capture_data_summary(new_image):
    '''
    Function that will add order information to the given
    record matching the document_name.
    '''

    record = valid_capture(new_image)

    wo_key = record['work_order_key']

    sblu = record.get("sblu", None)
    vin = record.get("vin", None)
    site_id = record.get("site_id", None)
    work_order_number = record.get("work_order_number", None)

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

    order_data = {}
    order_data.update({
        stringcase.snakecase(k): v for k, v in
        record["order"].items()
    })
    record_data = {
        'capture_status': order_data.get('status'),
        'capture_completed_timestamp': order_data.get('completed_timestamp'),
        "capture_updated": Decimal(time.time()),
        "sblu": sblu,
        "entity_type": "summary",
        "vin": vin,
        "site_id": site_id,
        "work_order_number": work_order_number,
        "updated": Decimal(time.time())
    }
    sk = f"workorder:{wo_key}"
    put_work_order(wo_key, sk, record_data)
