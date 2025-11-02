import pathlib
import json

import pytest
from boto3.dynamodb.conditions import Key
from unit_test.utils import call_lambda

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

event_file = (
    f"{CURRENT_DIR}/test_data_files/request/oracle_invoice_api_amp.json"
)
exp_response_file = f"{CURRENT_DIR}/test_data_files/expected_response/oracle_invoice_api_amp.json"
sk_prefix = "oracleinvoiceconsignmentdetails:36761568"


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((False, sk_prefix, exp_response_file), event_file, exp_response_file)],
    indirect=True,
)
def test_oracle_invoice_main_handler(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-rpp-recon-workorder-oracle-invoice-processor", get_event_data)

    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(get_exp_response_data["Records"][0]["pk"])
        & Key("sk").begins_with(sk_prefix)
    )

    for item in dynamodb_response["Items"]:
        item.pop("updated")
        item.pop("updated_hr")

    assert get_exp_response_data["Records"] == json.loads(json.dumps(dynamodb_response["Items"], default=str))
