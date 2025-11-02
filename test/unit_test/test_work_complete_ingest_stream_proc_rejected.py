import pathlib

import pytest
from boto3.dynamodb.conditions import Key
from unit_test.utils import call_lambda

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

LAMBDA_FN_NAME = "rpp-work-complete-ingest-stream-processor"

input_file = f"{CURRENT_DIR}/test_data_files/data/work_complete_ingest_prereq.json"
event_file = f"{CURRENT_DIR}/test_data_files/request/work_complete_ingest_stream_proc_rejected.json"
exp_response_file = f"{CURRENT_DIR}/test_data_files/expected_response/work_complete_ingest_stream_proc_rejected.json"


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((True, "clean_up", input_file), event_file, exp_response_file)],
    indirect=True,
)
def test_work_complete_ingest_stream_proc_work_rejected(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-{LAMBDA_FN_NAME}", get_event_data)

    print(get_exp_response_data["Records"][0]["pk"])
    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(get_exp_response_data["Records"][0]["pk"])
        & Key("sk").begins_with("workorder")
    )

    if dynamodb_response["Items"][0].get("work_rejected") and \
            dynamodb_response["Items"][0].get("work_order_complete"):
        assert True
    else:
        assert False

