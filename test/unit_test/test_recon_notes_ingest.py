import pathlib

import pytest
from unit_test.utils import (
    call_lambda,
    create_stream_event,
    get_json_content,
    get_dynamodb_record_begins_with_sk,
)

CURRENT_DIR = pathlib.Path(__file__).parent.resolve()

EXP_RESPONSE_FILE = (
    f"{CURRENT_DIR}/test_data_files/expected_response/"
    "rpp_notes_enhanced_event_handler.json"
)

# Test data for enhanced handler operations
TEST_DATA_ENHANCED = [
    # Scenario 1: Part note
    (
        [],
        f"{CURRENT_DIR}/test_data_files/request/notes/rpp_notes_enhanced_event_handler.json",
        {
            "pk": "workorder:12144808#QNM5",
            "sk": "notes:part#0523#BR#2025-06-16T14:42:38.625758Z",
        },
    ),
    # Scenario 2: damage note
    (
        [],
        f"{CURRENT_DIR}/test_data_files/request/notes/rpp_notes_enhanced_damage.json",
        {
            "pk": "workorder:12144808#QNM5",
            "sk": "notes:damage#0523#BR#2025-06-17T14:42:38.625758Z",
        },
    ),
    # Scenario 3: Workorder note
    (
        [],
        f"{CURRENT_DIR}/test_data_files/request/notes/rpp_notes_enhanced_workorder.json",
        {
            "pk": "workorder:12144808#QNM5",
            "sk": "notes:2025-06-18T14:42:38.625758Z",
        },
    ),
]


def verify_enhanced_notes_records(table, expected_data, count=1):
    """Verify enhanced notes records with comprehensive validation"""
    if expected_data:
        items = get_dynamodb_record_begins_with_sk(
            pk=expected_data["pk"],
            sk=expected_data["sk"],
            tbl=table,
        )
        assert len(items) == count, f"Expected {count} item(s), found {len(items)}"

        if count == 0:
            return

        # If we expect one item, perform detailed checks
        item = items[0]

        # Verify the updated field length is 13 digits before the decimal point
        assert (
            len(str(item["updated"]).split(".")[0]) == 13
        ), f"Updated timestamp format incorrect: {item['updated']}"

        # Verify required enhanced fields are present
        required_fields = [
            "pk",
            "sk",
            "note",
            "source",
            "user_id",
            "type",
            "work_order_number",
            "sblu",
            "vin",
            "site_id",
        ]
        for field in required_fields:
            assert field in item, f"Required field '{field}' missing from record"


@pytest.mark.parametrize(
    "module_setup_and_teardown, event_file, expected_changes",
    TEST_DATA_ENHANCED,
    indirect=["module_setup_and_teardown"],
)
def test_rpp_notes_enhanced_event_handler(
    module_setup_and_teardown,
    cleanup_created_records,
    alias,
    get_main_table,
    event_file,
    expected_changes,
    request,
):
    """Test enhanced notes handler functionality"""
    # Get test data
    body = get_json_content(event_file)

    # Create Kinesis stream event using the SDK
    event_payload = create_stream_event(body)

    # Add pk to cleanup list
    if expected_changes.get("pk"):
        request.node.add_created_record(expected_changes["pk"])

    try:
        call_lambda(
            f"{alias}-rpp-workorder-notes-kstream-processor",
            event_payload,
        )
    except Exception as e:
        pytest.fail(f"Lambda function failed unexpectedly: {str(e)}")

    # Verify results
    if expected_changes:
        verify_enhanced_notes_records(
            get_main_table,
            expected_changes,
        )


# Test data for error scenarios - these should handle failures gracefully
TEST_DATA_ERROR_SCENARIOS = [
    # Scenario 1: Missing required field (note)
    (
        [],
        f"{CURRENT_DIR}/test_data_files/request/notes/rpp_notes_enhanced_missing_note.json",
        None,  # No expected changes as this should fail validation
    ),
]


@pytest.mark.parametrize(
    "module_setup_and_teardown, event_file, expected_changes",
    TEST_DATA_ERROR_SCENARIOS,
    indirect=["module_setup_and_teardown"],
)
def test_rpp_notes_enhanced_event_handler_error_scenarios(
    module_setup_and_teardown,
    alias,
    get_main_table,
    event_file,
    expected_changes,
):
    """Test enhanced notes handler error scenarios and validation failures"""
    # Get test data
    body = get_json_content(event_file)

    # Create Kinesis stream event using the SDK
    event_payload = create_stream_event(body)

    # For error scenarios, we expect the lambda to handle the error gracefully
    # The lambda should not fail but should process the error appropriately
    try:
        response = call_lambda(
            f"{alias}-rpp-notes-enhanced-event-handler",
            event_payload,
        )

        # For validation errors, the lambda should return batch item failures
        if response and "batchItemFailures" in response:
            # This is expected for validation errors
            assert (
                len(response["batchItemFailures"]) > 0
            ), "Expected batch item failures for invalid data"

    except Exception as e:
        # Some validation errors might cause the lambda to fail completely
        # This is also acceptable behavior for invalid input
        assert (
            "validation" in str(e).lower() or "invalid" in str(e).lower()
        ), f"Unexpected error type: {str(e)}"

    # Verify no records were created for invalid data
    if expected_changes is None:
        # For error scenarios, we shouldn't create any records
        verify_enhanced_notes_records(
            get_main_table,
            {"pk": "workorder:12144808#QNM5", "sk": "notes:"},
            count=0,
        )
