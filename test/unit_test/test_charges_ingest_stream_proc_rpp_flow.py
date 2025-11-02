import pathlib
import json

import pytest
from boto3.dynamodb.conditions import Key
from unit_test.utils import call_lambda

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

event_file = (
    f"{CURRENT_DIR}/test_data_files/request/charges_ingest_stream_proc_rpp_flow.json"
)
exp_response_file = f"{CURRENT_DIR}/test_data_files/expected_response/charges_ingest_stream_proc_rpp_flow.json"
sk_prefix = "chargecall:delivery"


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((False, sk_prefix, exp_response_file), event_file, exp_response_file)],
    indirect=True,
)
def test_charges_ingest_rpp_flow(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", get_event_data)

    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(get_exp_response_data["Records"][0]["pk"])
        & Key("sk").begins_with(sk_prefix)
    )

    for item in dynamodb_response["Items"]:
        item.pop("updated")

    assert get_exp_response_data["Records"] == json.loads(json.dumps(dynamodb_response["Items"], default=str))
