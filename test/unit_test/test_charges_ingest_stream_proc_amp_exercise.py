import pathlib

import pytest
from boto3.dynamodb.conditions import Key
from unit_test.utils import call_lambda

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

event_file = (
    f"{CURRENT_DIR}/test_data_files/request/charges_ingest_stream_proc_amp_exercise.json"
)
exp_response_file = f"{CURRENT_DIR}/test_data_files/expected_response/charges_ingest_stream_proc_amp_exercise.json"
fee_name = "charge:exercise_fee#"


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((False, fee_name, exp_response_file), event_file, exp_response_file)],
    indirect=True,
)
def test_charges_ingest_stream_proc_amp_exercise(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", get_event_data)

    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(get_exp_response_data["Records"][0]["pk"])
        & Key("sk").begins_with(fee_name)
    )

    for item in dynamodb_response["Items"]:
        item.pop("sk")
        item.pop("charge_datetime", None)
        item.pop("updated", None)
        item.get("active_tasks", {}).pop("created_on", None)

    assert get_exp_response_data["Records"] == dynamodb_response["Items"]
