import json
import pathlib

import pytest
from boto3.dynamodb.conditions import Key
from unit_test.utils import call_lambda

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

event_file = f"{CURRENT_DIR}/test_data_files/request/recon_labor_ingest.json"
event_estimate_file = f"{CURRENT_DIR}/test_data_files/request/recon_labor_estimate_ingest.json"
exp_response_file = (
    f"{CURRENT_DIR}/test_data_files/expected_response/recon_labor_ingest.json"
)
exp_response_estimate_file = (
    f"{CURRENT_DIR}/test_data_files/expected_response/recon_labor_estimate_ingest.json"
)

event_file_rt_no_cr = f"{CURRENT_DIR}/test_data_files/request/recon_labor_ingest_repair_tracker_no_cr.json"
exp_response_file_rt_no_cr = (
    f"{CURRENT_DIR}/test_data_files/expected_response/recon_labor_ingest_repair_tracker_no_cr.json"
)

event_file_rt = f"{CURRENT_DIR}/test_data_files/request/recon_labor_ingest_repair_tracker.json"
exp_response_file_rt = (
    f"{CURRENT_DIR}/test_data_files/expected_response/recon_labor_ingest_repair_tracker.json"
)

sk = "repair_labor_status:0520#09#CO#"
sk_est = "labor_estimate:0520#09#LS#RE#RE#rpp-wpe#33"
sk_rt = "repair_labor_status:0061#00#BR#RQ#RP"
sk_no_cr = "repair_labor_status_no_cr:0061#00#BR#RQ#RP"

# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((False, sk, exp_response_file), event_file, exp_response_file)],
    indirect=True,
)
def test_labor_ingest_kstream_processor(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-rpp-recon-workorder-labor-ingest-kstream-processor", get_event_data)

    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(
            get_exp_response_data["Records"][0]["pk"]
        )
        & Key("sk").begins_with(sk)
    )

    for item in dynamodb_response["Items"]:
        item.pop("updated")
        item.pop("updated_hr")
        item.pop("approximate_receive_count")
        item.pop("approximate_reprocessing_count")
        item.pop("event_body", None)

    assert get_exp_response_data["Records"][0] in json.loads(
        json.dumps(dynamodb_response["Items"], default=str)
    )


@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((False, sk_est, exp_response_estimate_file), event_estimate_file, exp_response_estimate_file)],
    indirect=True,
)
def test_labor_estimate_ingest_kstream_processor(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-rpp-recon-workorder-labor-ingest-kstream-processor", get_event_data)

    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(
            get_exp_response_data["Records"][0]["pk"]
        )
        & Key("sk").begins_with(sk_est)
    )

    for item in dynamodb_response["Items"]:
        item.pop("updated", None)
        item.pop("hr_updated", None)
        item.pop("updated_hr", None)
        item.pop("estimate_status_updated", None)
        item.pop("invalid_fields", None)

    assert get_exp_response_data["Records"][0] in json.loads(
        json.dumps(dynamodb_response["Items"], default=str)
    )


@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((False, sk_no_cr, exp_response_file_rt_no_cr), event_file_rt_no_cr, exp_response_file_rt_no_cr)],
    indirect=True,
)
def test_labor_ingest_kstream_processor_repair_tracker_no_cr(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-rpp-recon-workorder-labor-ingest-kstream-processor", get_event_data)


    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(
            get_exp_response_data["Records"][0]["pk"]
        )
        & Key("sk").begins_with(sk_no_cr)
    )

    for item in dynamodb_response["Items"]:
        item.pop("updated")
        item.pop("updated_hr")
        item.pop("updated_repair_execution", None)
        item.pop("approximate_receive_count", None)
        item.pop("approximate_reprocessing_count", None)
        item.pop("event_body", None)

    assert get_exp_response_data["Records"][0] in json.loads(
        json.dumps(dynamodb_response["Items"], default=str)
    )



@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((False, sk_rt, exp_response_file_rt), event_file_rt, exp_response_file_rt)],
    indirect=True,
)
def test_labor_ingest_kstream_processor_repair_tracker(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-rpp-recon-workorder-labor-ingest-kstream-processor", get_event_data)


    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(
            get_exp_response_data["Records"][0]["pk"]
        )
        & Key("sk").begins_with(sk_rt)
    )

    for item in dynamodb_response["Items"]:
        item.pop("updated")
        item.pop("updated_hr")
        item.pop("updated_repair_execution", None)
        item.pop("approximate_receive_count", None)
        item.pop("approximate_reprocessing_count", None)
        item.pop("event_body", None)

    assert get_exp_response_data["Records"][0] in json.loads(
        json.dumps(dynamodb_response["Items"], default=str)
    )
