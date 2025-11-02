"""
Test suite for charges_ingest.py manheim_account_number upsert functionality.

This module tests the following scenarios:
1. Initial insertion of charge records with manheim_account_number upserting to workorder records
2. Updates when manheim_account_number changes in charge records
3. No updates when manheim_account_number remains the same (conditional check prevention)
4. Batch processing behavior with multiple charges for the same workorder
5. Non-charge records not triggering manheim_account_number updates

The tests verify that the charges_ingest lambda properly maintains manheim_account_number
consistency between charge records and their corresponding workorder records.
"""

import pathlib
import json
import logging as LOGGER
import pytest
from boto3.dynamodb.conditions import Key
from unit_test.utils import (
    call_lambda,
    create_dynamo_kinesis_event,
    get_json_content,
    dynamo_delete_records,
)

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

# Test data files for manheim_account_number upsert testing
charge_insert_event_file = f"{CURRENT_DIR}/test_data_files/request/charges_ingest_manheim_account_number_insert.json"
exp_response_file = f"{CURRENT_DIR}/test_data_files/expected_response/charges_ingest_manheim_account_number.json"

sk_prefix = "charge:STO#"


@pytest.mark.parametrize(
    "setup_and_teardown, get_event_data, get_exp_response_data",
    [
        (
            (False, sk_prefix, exp_response_file),
            charge_insert_event_file,
            exp_response_file,
        )
    ],
    indirect=True,
)
def test_charges_ingest_manheim_account_number_insert(
    setup_and_teardown, get_event_data, get_exp_response_data, alias, get_table
):
    """
    Test that when a charge record with manheim_account_number is inserted,
    the manheim_account_number is properly upserted to the workorder record.
    """

    # Call the lambda function with the charge insert event
    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", get_event_data)

    # Get the expected charge record from test data
    expected_charge_record = get_exp_response_data["Records"][0]
    workorder_pk = expected_charge_record["pk"]

    # Verify the charge record was created properly
    charge_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(workorder_pk)
        & Key("sk").begins_with(sk_prefix)
    )

    LOGGER.info("Charge record created:")
    LOGGER.info(json.dumps(charge_response, default=str, indent=4, sort_keys=True))

    assert len(charge_response["Items"]) == 1

    # Verify the workorder record was updated with manheim_account_number
    workorder_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(workorder_pk) & Key("sk").eq(workorder_pk)
    )

    LOGGER.info("Workorder record with manheim_account_number:")
    LOGGER.info(json.dumps(workorder_response, default=str, indent=4, sort_keys=True))

    assert len(workorder_response["Items"]) == 1
    workorder_record = workorder_response["Items"][0]

    # Verify manheim_account_number was set correctly
    assert (
        workorder_record["manheim_account_number"]
        == expected_charge_record["manheim_account_number"]
    )
    assert "updated" in workorder_record
    assert "updated_hr" in workorder_record


def test_charges_ingest_manheim_account_number_update(alias, get_table):
    """
    Test that when a charge record is modified with a different manheim_account_number,
    the workorder record is updated with the new value.
    """

    # Step 1: Insert initial charge record with manheim_account_number
    expected_response = get_json_content(exp_response_file)
    record = expected_response["Records"][0]
    workorder_pk = record["pk"]

    kinesis_event = create_dynamo_kinesis_event(event_name="INSERT", record=record)
    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", kinesis_event)

    # Verify initial manheim_account_number is set
    workorder_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(workorder_pk) & Key("sk").eq(workorder_pk)
    )

    assert len(workorder_response["Items"]) == 1
    initial_workorder = workorder_response["Items"][0]
    assert (
        initial_workorder["manheim_account_number"] == record["manheim_account_number"]
    )
    initial_updated_time = initial_workorder["updated"]

    # Step 2: Update charge record with different manheim_account_number
    new_manheim_account_number = "9999999"
    kinesis_event = create_dynamo_kinesis_event(
        event_name="MODIFY",
        record=record,
        modified_data={"manheim_account_number": new_manheim_account_number},
        removed_data={},
    )
    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", kinesis_event)

    # Verify manheim_account_number was updated
    workorder_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(workorder_pk) & Key("sk").eq(workorder_pk)
    )

    assert len(workorder_response["Items"]) == 1
    updated_workorder = workorder_response["Items"][0]
    assert updated_workorder["manheim_account_number"] == new_manheim_account_number
    assert updated_workorder["updated"] > initial_updated_time  # Should be updated

    # Cleanup
    dynamo_delete_records(pk=workorder_pk, tbl=get_table)


def test_charges_ingest_manheim_account_number_no_change(alias, get_table):
    """
    Test that when a charge record is modified but manheim_account_number stays the same,
    the workorder record is not updated (conditional check should prevent update).
    """

    # Step 1: Insert initial charge record with manheim_account_number
    expected_response = get_json_content(exp_response_file)
    record = expected_response["Records"][0]
    workorder_pk = record["pk"]

    kinesis_event = create_dynamo_kinesis_event(event_name="INSERT", record=record)
    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", kinesis_event)

    # Get initial state
    workorder_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(workorder_pk) & Key("sk").eq(workorder_pk)
    )

    assert len(workorder_response["Items"]) == 1
    initial_workorder = workorder_response["Items"][0]
    initial_updated_time = initial_workorder["updated"]

    # Step 2: Modify charge record but keep same manheim_account_number
    kinesis_event = create_dynamo_kinesis_event(
        event_name="MODIFY",
        record=record,
        modified_data={"some_other_field": "changed_value"},  # Change other field
        removed_data={},
    )
    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", kinesis_event)

    # Verify manheim_account_number was NOT updated (conditional check should prevent it)
    workorder_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(workorder_pk) & Key("sk").eq(workorder_pk)
    )

    assert len(workorder_response["Items"]) == 1
    final_workorder = workorder_response["Items"][0]
    assert final_workorder["manheim_account_number"] == record["manheim_account_number"]
    # Updated time should be the same since conditional check should prevent update
    assert final_workorder["updated"] == initial_updated_time

    # Cleanup
    dynamo_delete_records(pk=workorder_pk, tbl=get_table)


def test_charges_ingest_manheim_account_number_multiple_charges_same_batch(
    alias, get_table
):
    """
    Test that when multiple charge records for the same workorder are processed in the same batch,
    only the first one updates the manheim_account_number (records_dict prevents duplicate updates).
    """

    # Create two charge records for the same workorder
    expected_response = get_json_content(exp_response_file)
    record1 = expected_response["Records"][0].copy()
    record2 = expected_response["Records"][0].copy()

    # Different charge IDs but same workorder
    record1["sk"] = "charge:STO#DSTOS#charge1"
    record2["sk"] = "charge:STO#DSTOS#charge2"
    record2["manheim_account_number"] = "8888888"  # Different account number

    workorder_pk = record1["pk"]

    # Create kinesis events for both records
    kinesis_event1 = create_dynamo_kinesis_event(event_name="INSERT", record=record1)
    kinesis_event2 = create_dynamo_kinesis_event(event_name="INSERT", record=record2)

    # Create a batch event with both records
    batch_event = {
        "Records": [kinesis_event1["Records"][0], kinesis_event2["Records"][0]]
    }

    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", batch_event)

    # Verify both charge records were created
    charge_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(workorder_pk)
        & Key("sk").begins_with("charge:")
    )

    assert len(charge_response["Items"]) == 2

    # Verify workorder was updated with the first record's manheim_account_number
    workorder_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(workorder_pk) & Key("sk").eq(workorder_pk)
    )

    assert len(workorder_response["Items"]) == 1
    workorder_record = workorder_response["Items"][0]

    # Should have the first record's manheim_account_number, not the second
    assert (
        workorder_record["manheim_account_number"] == record1["manheim_account_number"]
    )

    # Cleanup
    dynamo_delete_records(pk=workorder_pk, tbl=get_table)


def test_charges_ingest_non_charge_record_no_update(alias, get_table):
    """
    Test that non-charge records (sk not starting with 'charge') do not trigger
    manheim_account_number updates.
    """

    # Create a non-charge record
    expected_response = get_json_content(exp_response_file)
    record = expected_response["Records"][0].copy()
    record["sk"] = "some_other_record_type"  # Not starting with "charge"

    workorder_pk = record["pk"]

    kinesis_event = create_dynamo_kinesis_event(event_name="INSERT", record=record)
    call_lambda(f"{alias}-rpp-charges-ingest-stream-processor", kinesis_event)

    # Verify the record was created
    record_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(workorder_pk)
        & Key("sk").eq("some_other_record_type")
    )

    assert len(record_response["Items"]) == 1

    # Verify no workorder record was created/updated with manheim_account_number
    workorder_response = get_table.query(
        KeyConditionExpression=Key("pk").eq(workorder_pk) & Key("sk").eq(workorder_pk)
    )

    # Should be empty since non-charge records don't trigger workorder updates
    assert len(workorder_response["Items"]) == 0

    # Cleanup
    dynamo_delete_records(pk=workorder_pk, tbl=get_table)
