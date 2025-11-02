# This file is used to import the arguments from the command line
# and to setup the pytest fixtures. The fixtures are used to setup
# the test data and to call the lambda function.
import json
import logging as LOGGER

import boto3
import pytest
from boto3.dynamodb.conditions import Key

from unit_test.utils import add_dynamodb_data, cleanup, cleanup_by_pk

DYNAMODB_CLIENT = boto3.resource("dynamodb", region_name="us-east-1")


def pytest_addoption(parser):
    parser.addoption("--alias", action="store", help="input alias")


@pytest.fixture(scope="session")
def params(request):
    params = {"alias": request.config.getoption("--alias")}
    if params["alias"] is None:
        pytest.skip()
    return params


@pytest.fixture(scope="session")
def alias(pytestconfig):
    return pytestconfig.getoption("alias")


@pytest.fixture()
def get_table(alias):
    return DYNAMODB_CLIENT.Table(f"{alias}-rpp-recon-work-order")


@pytest.fixture()
def get_main_table(alias):
    """Main table fixture - same as get_table for now"""
    return DYNAMODB_CLIENT.Table(f"{alias}-rpp-recon-work-order")


@pytest.fixture()
def cleanup_created_records(request, get_main_table):
    """Fixture to clean up records created during tests"""
    created_records = []

    # Add method to request node to track created records
    def add_created_record(pk):
        created_records.append(pk)

    request.node.add_created_record = add_created_record

    yield

    # Cleanup after test
    for pk in created_records:
        try:
            response = get_main_table.query(KeyConditionExpression=Key("pk").eq(pk))
            for record in response.get("Items", []):
                sk = record["sk"]
                get_main_table.delete_item(Key={"pk": pk, "sk": sk})
                LOGGER.debug(f"Cleaned up record: pk={pk}, sk={sk}")
        except Exception as e:
            LOGGER.warning(f"Failed to cleanup record pk={pk}: {e}")


@pytest.fixture()
def get_event_data(request):
    if (len(request.param)) == 0:
        return None
    else:
        file = request.param
        with open(file) as json_file:
            payload = json.load(json_file)
        json_file.close()
        return payload


@pytest.fixture()
def get_exp_response_data(request):
    file = request.param

    with open(file) as jsonFile:
        payload = json.load(jsonFile)
    jsonFile.close()

    return payload


@pytest.fixture()
def setup_and_teardown(request, get_table):
    input_flag, sk, file = request.param
    LOGGER.debug("\n ************ setup ************")
    if input_flag:
        add_dynamodb_data(file, get_table)
    yield
    if sk:
        if sk == "clean_up":
            cleanup_by_pk(file, get_table)
        else:
            cleanup(file, sk, get_table)

    LOGGER.debug("\n ************ teardown ************")


@pytest.fixture(scope="module")
def module_setup_and_teardown(request, get_main_table):
    setup_files = getattr(
        request, "param", []
    )  # Default to empty list if no parameters provided

    # Setup
    for file in setup_files:
        add_dynamodb_data(file, get_main_table)

    yield

    # Teardown
    for file in reversed(setup_files):
        cleanup(file, get_main_table)
