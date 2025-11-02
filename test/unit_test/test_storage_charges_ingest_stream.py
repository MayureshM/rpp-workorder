import pathlib

import pytest
from boto3.dynamodb.conditions import Key
from unit_test.utils import call_lambda

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

LAMBDA_FN_NAME = "rpp-storage-charges-ingest-stream-processor"

input_file = f"{CURRENT_DIR}/test_data_files/data/storage_charges_ingest_stream.json"
event_file_daily_storage_insert = f"{CURRENT_DIR}/test_data_files/request/daily_storage_charges_ingest_stream.json"
event_file_future_insert = f"{CURRENT_DIR}/test_data_files/request/storage_charges_ingest_stream_future_insert.json"
event_file_future_modify = f"{CURRENT_DIR}/test_data_files/request/storage_charges_ingest_stream_future_modify.json"
event_file_past_insert = f"{CURRENT_DIR}/test_data_files/request/storage_charges_ingest_stream_past_insert.json"
event_file_past_modify = f"{CURRENT_DIR}/test_data_files/request/storage_charges_ingest_stream_past_modify.json"
exp_response_file_move_ahead = f"{CURRENT_DIR}/test_data_files/expected_response/storage_charges_ingest_stream_move_ahead.json"
exp_response_file_move_behind = f"{CURRENT_DIR}/test_data_files/expected_response/storage_charges_ingest_stream_move_behind.json"
exp_response_file_daily_storage_insert = f"{CURRENT_DIR}/test_data_files/expected_response/daily_storage_charges_ingest_stream.json"


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((True, "clean_up", input_file), event_file_daily_storage_insert, exp_response_file_daily_storage_insert)],
    indirect=True,
)
def test_daily_storage_charges_ingest_stream(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-{LAMBDA_FN_NAME}", get_event_data)

    print(get_exp_response_data["Records"][0]["pk"])
    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(get_exp_response_data["Records"][0]["pk"])
        & Key("sk").begins_with("workorder")
    )
    print({"DynamoDB Response": dynamodb_response,
           "Expected Response": get_exp_response_data["Records"][0]["storage_date"]})

    assert dynamodb_response["Items"][0].get("storage_date", "1970-01-01") \
        == get_exp_response_data["Records"][0]["storage_date"]


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((True, "clean_up", input_file), event_file_future_insert, exp_response_file_move_ahead)],
    indirect=True,
)
def test_storage_charges_ingest_stream_future_insert(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-{LAMBDA_FN_NAME}", get_event_data)

    print(get_exp_response_data["Records"][0]["pk"])
    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(get_exp_response_data["Records"][0]["pk"])
        & Key("sk").begins_with("workorder")
    )
    print({"DynamoDB Response": dynamodb_response,
           "Expected Response": get_exp_response_data["Records"][0]["storage_date"]})

    assert dynamodb_response["Items"][0].get("storage_date", "1970-01-01") \
        == get_exp_response_data["Records"][0]["storage_date"]


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((True, "clean_up", input_file), event_file_past_insert, exp_response_file_move_behind)],
    indirect=True,
)
def test_storage_charges_ingest_stream_past_insert(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-{LAMBDA_FN_NAME}", get_event_data)

    print(get_exp_response_data["Records"][0]["pk"])
    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(get_exp_response_data["Records"][0]["pk"])
        & Key("sk").begins_with("workorder")
    )
    print({"DynamoDB Response": dynamodb_response,
           "Expected Response": get_exp_response_data["Records"][0]["storage_date"]})

    assert dynamodb_response["Items"][0].get("storage_date", "1970-01-01") \
        == get_exp_response_data["Records"][0]["storage_date"]


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((True, "clean_up", input_file), event_file_future_modify, exp_response_file_move_ahead)],
    indirect=True,
)
def test_storage_charges_ingest_stream_future_modify(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-{LAMBDA_FN_NAME}", get_event_data)

    print(get_exp_response_data["Records"][0]["pk"])
    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(get_exp_response_data["Records"][0]["pk"])
        & Key("sk").begins_with("workorder")
    )
    print({"DynamoDB Response": dynamodb_response,
           "Expected Response": get_exp_response_data["Records"][0]["storage_date"]})

    assert dynamodb_response["Items"][0].get("storage_date", "1970-01-01") \
        == get_exp_response_data["Records"][0]["storage_date"]


# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((True, "clean_up", input_file), event_file_past_modify, exp_response_file_move_behind)],
    indirect=True,
)
def test_storage_charges_ingest_stream_past_modify(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    call_lambda(f"{alias}-{LAMBDA_FN_NAME}", get_event_data)

    print(get_exp_response_data["Records"][0]["pk"])
    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(get_exp_response_data["Records"][0]["pk"])
        & Key("sk").begins_with("workorder")
    )
    print({"DynamoDB Response": dynamodb_response,
           "Expected Response": get_exp_response_data["Records"][0]["storage_date"]})

    assert dynamodb_response["Items"][0].get("storage_date", "1970-01-01") \
        == get_exp_response_data["Records"][0]["storage_date"]
