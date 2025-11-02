import json
import pathlib

import pytest
from boto3.dynamodb.conditions import Key
from unit_test.utils import call_lambda

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

event_file = (
    f"{CURRENT_DIR}/test_data_files/request/order_image.json"
)
exp_response_file = f"{CURRENT_DIR}/test_data_files/expected_response/order_image.json"


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((False, None, exp_response_file), event_file, exp_response_file)],
    indirect=True,
)
def test_charges_ingest_stream_order_image(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-rpp-workorder-order-image-kstream-processor", get_event_data)

    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(get_exp_response_data["pk"])
        & Key("sk").eq(get_exp_response_data["sk"])
    )

    assert dynamodb_response["Items"][0].get("imaging_status", "") == "COMPLETED"
