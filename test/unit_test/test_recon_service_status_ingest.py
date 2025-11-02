import json
import pathlib

import pytest
from boto3.dynamodb.conditions import Key
from unit_test.utils import call_lambda

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

event_file = f"{CURRENT_DIR}/test_data_files/request/recon_service_status_ingest.json"
exp_response_file = (
    f"{CURRENT_DIR}/test_data_files/expected_response/recon_service_status_ingest.json"
)
sk = "service_status:MECHM"


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((False, sk, exp_response_file), event_file, exp_response_file)],
    indirect=True,
)
def test_service_status_ingest_stream(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-rpp-recon-workorder-service-status-ingest-processor", get_event_data)

    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(
            get_exp_response_data["Records"][0]["pk"]
        )
        & Key("sk").begins_with(sk)
    )

    for item in dynamodb_response["Items"]:
        item.pop("updated")
        item.pop("updated_hr")
        item.pop("event_type", None)
        item.pop("event_name", None)

    assert get_exp_response_data["Records"][0] in json.loads(
        json.dumps(dynamodb_response["Items"], default=str)
    )
