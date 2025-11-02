import pathlib
import json
import logging as LOGGER

import pytest
from boto3.dynamodb.conditions import Key
from unit_test.utils import call_lambda, assert_equal, create_dynamo_kinesis_event, get_json_content, dynamo_delete_records

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

storage_insert_event_file = (
    f"{CURRENT_DIR}/test_data_files/request/charges_ingest_stream_proc_rpp_flow_daily_storage_insert.json"
)

exp_response_file = f"{CURRENT_DIR}/test_data_files/expected_response/charges_ingest_stream_proc_rpp_daily_storage_charge.json"
sk_prefix = "charge:STO#DSTOS#"

# Assert
@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [((False, sk_prefix, exp_response_file), storage_insert_event_file, exp_response_file)],
    indirect=True,
)
def test_charges_ingest_rpp__daily_storage_insert(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table, dynamo_response_records=None
):
    """
        This test is to verify rpp-workorder processes a charge record INSERT event, it creates a daily storage charge record
    """

    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", get_event_data)

    expected_charge_record = get_exp_response_data["Records"][0]

    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(expected_charge_record["pk"])
        & Key("sk").begins_with(sk_prefix)
    )
    LOGGER.info("charge record dynamodb_response")
    LOGGER.info(json.dumps(dynamodb_response, default=str, indent=4, sort_keys=True))

    assert len(dynamodb_response["Items"]) == len(get_exp_response_data["Records"])
    dynamodb_response_charge_record = dynamodb_response["Items"][0]

    LOGGER.info("get_exp_response_data")
    LOGGER.info(get_exp_response_data["Records"])

    assert_equal(expected_charge_record, dynamodb_response_charge_record, ignore_fields = ["updated", "charge_datetime"])


def test_charges_ingest_rpp__daily_storage_modify(alias, get_table):
    """
            This test is to verify rpp-workorder processes a charge record MODIFY event, it updates a daily storage charge record.
            In the scenario, the charge record old image has storage_end_date (or other fields) but the new image does not,
            The charge record should be modified, so that storage end date (or other removed fields) is not set in the daily storage charge record.
    """

    # Step 1: process a charge record INSERT event,  the charge record has storage_end_date
    expected_response = get_json_content(exp_response_file)
    LOGGER.info("expected_response")
    LOGGER.info(expected_response)
    record = expected_response["Records"][0]
    LOGGER.info(record)

    kinesis_event = create_dynamo_kinesis_event(event_name="INSERT", record=record)
    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", kinesis_event)

    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(record["pk"])
                               & Key("sk").begins_with(sk_prefix)
    )
    LOGGER.debug("charge record dynamodb_response")
    LOGGER.debug(json.dumps(dynamodb_response, default=str, indent=4, sort_keys=True))

    # verify the daily storage record is inserted, and has storage_end_date
    dynamodb_response_charge_record = dynamodb_response["Items"][0]
    assert dynamodb_response_charge_record["storage_end_date"]

    # Step 2: process the charge record MODIFY event, old image has storage_end_date, new image does not
    kinesis_event = create_dynamo_kinesis_event(
        event_name="MODIFY",
        record=record,
        modified_data={},
        removed_data={"storage_end_date": "2025-03-06",}
    )
    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", kinesis_event)

    dynamodb_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(record["pk"])
        & Key("sk").begins_with(sk_prefix)
    )

    LOGGER.debug("charge record dynamodb_response 2")
    LOGGER.debug(json.dumps(dynamodb_response, default=str, indent=4, sort_keys=True))

    # verify the daily storage record is modified, field storage_end_date is removed
    dynamodb_response_charge_record = dynamodb_response["Items"][0]
    assert not dynamodb_response_charge_record.get("storage_end_date")

    expected_charge_record = get_json_content(exp_response_file)["Records"][0]
    expected_charge_record.pop("storage_end_date", None)

    assert_equal(expected_charge_record, dynamodb_response_charge_record, ignore_fields = ["updated", "charge_datetime"])
    dynamo_delete_records(pk=expected_charge_record["pk"], tbl=get_table)