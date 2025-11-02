"""
utility for common methods used across handlers
"""

import logging
import json
from stringcase import snakecase
import time as _time
from datetime import datetime, timezone
from decimal import Decimal
from functools import wraps
from aws_lambda_powertools import Logger, Tracer


import boto3

# Set Logging Level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

LOGGER = Logger()
TRACER = Tracer()


def verify_field(body, field_name, body_name):
    """
    verifies the field exists within the body(dict) based in
        if found returns the value at body[field_name]
        else blank string ('')
    """

    logger.info("Searching for %s in %s", field_name, body_name)
    if field_name not in body:
        logger.error("Incorrect data (missing %s)", field_name)
        return False, ""
    return True, body[field_name]


def get_parameter(name, decryption):
    """
    Get params
    """
    session = boto3.Session(region_name="us-east-1")
    ssm = session.client("ssm")
    parameter_response = ssm.get_parameter(Name=name, WithDecryption=decryption)

    if "Parameter" not in parameter_response:
        return ""

    parameter = parameter_response.get("Parameter")

    value = parameter.get("Value")

    logger.info("GET parameter: " + name + " = " + value)

    return value


def verify_empty_field(body, body_name):
    """
    verifies the the body(array) is not empty based in
    if non empty returns the value at body[0]
    else blank string ('')
    """

    logger.info("Verifying the array %s is not empty", body_name)
    if len(body) == 0:
        logger.error("Empty array ")
        return False, ""
    return True, body[0]


def get_vin(record):
    # Check all 17 VIN digits (11 + 6)
    vin_parts_present = all(
        'vin' + str(i) in record and record['vin' + str(i)] is not None
        for i in range(1, 12)
    ) and ('vin_last_6' in record and record['vin_last_6'] is not None)

    if vin_parts_present:
        # Join first 11 digits
        vin = ''.join([
            record['vin' + str(i)] for i in range(1, 12)
        ])
        print("Constructed VIN from parts:", vin)
        # Add last 6 digits
        vin += record.pop('vin_last_6')
        for i in range(1, 12):
            record.pop('vin' + str(i))
        record.pop('previousVin', None)
        return vin
    elif 'previousVin' in record and record['previousVin'] is not None:
        return record.pop('previousVin')
    else:
        return None


def add_update_attributes(record, attributes):
    current_time = Decimal(_time.time())
    for attribute in attributes:
        record.update({attribute: current_time})


def get_utc_now():
    return datetime.now(timezone.utc)


def get_updated_hr(updated):
    """
    helper to convert the epoch timestamp to a human readable format in utc
    """
    return updated.strftime("%Y-%m-%d %H:%M:%S.%f")


def get_updated_source_hr(updated):
    """
    helper to convert the epoch timestamp to a human readable format in utc
    """
    return datetime.fromtimestamp(int(updated) / 1000, timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )


def log_execution_time(event_name: str = ""):
    """
    Decorator to log function execution time
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = _time.monotonic()
            result = func(*args, **kwargs)
            end = _time.monotonic()
            data = {
                "event": event_name if event_name else func.__name__,
                "message": f"Conclude processing {func.__name__}",
                "loop_time": end - start,
            }
            logger.info(data)
            return result

        return wrapper

    return decorator


def add_tracer_metadata_to_current_subsegment(request):
    try:
        current_subsegment = TRACER.provider.current_subsegment()
        current_subsegment.put_metadata("record", request)
    except AttributeError as attr_err:
        LOGGER.exception(
            {
                "message": "Failed to send metadata to xray",
                "reason": str(attr_err),
                "request": safe_json_for_logging(request),
            }
        )


def add_tracer_annotation_to_current_subsegment(request, source="Recon Workorder"):
    try:
        current_subsegment = TRACER.provider.current_subsegment()
        current_subsegment.put_annotation("source", source)
        [
            current_subsegment.put_annotation(key, value)
            for key, value in request.items()
        ]
    except AttributeError as attr_err:
        LOGGER.exception(
            {
                "message": "Failed to send annotation to xray",
                "reason": str(attr_err),
                "request": safe_json_for_logging(request),
            }
        )


def add_tracer_exception_to_current_subsegment(error):
    try:
        current_subsegment = TRACER.provider.current_subsegment()
        current_subsegment.add_error_flag()
        current_subsegment.add_fault_flag()
        current_subsegment.apply_status_code(500)
        current_subsegment.add_exception(error, [], True)
    except AttributeError as attr_err:
        LOGGER.exception(
            {
                "message": "Failed to send exception to xray",
                "reason": str(attr_err),
                "error": safe_json_for_logging(error),
            }
        )


def safe_json_for_logging(data):
    """
    Safely serialize data for logging to prevent log injection attacks.

    This function addresses AWS CodeGuru recommendation for CWE-117 (Log Injection)
    by using json.dumps() which automatically escapes dangerous characters.

    Args:
        data: The data to serialize safely

    Returns:
        JSON string representation safe for logging
    """
    try:
        # json.dumps automatically escapes newlines, quotes, and other dangerous characters
        return json.dumps(data, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        # Fallback for non-serializable objects
        return f"[NON_SERIALIZABLE: {type(data).__name__}]"


def get_removed_attributes(new, old, record_mapping=None):
    if record_mapping is None:
        record_mapping = {}
    remove_attributes = []
    for key in list(filter(lambda x: x not in set(new), old)):
        key = record_mapping.get(key, snakecase(key))
        remove_attributes.append(key)
    return remove_attributes


def sanitize_for_logging(data, max_length=200, max_depth=10, current_depth=0):
    """
    Recursively sanitize data for safe logging to prevent log injection attacks.

    This function removes control characters, newlines, and other dangerous characters
    that could be used for log injection (CWE-117).

    Args:
        data: The data to sanitize (str, list, dict, or any other type)
        max_length: Maximum length for string values (default: 200)
        max_depth: Maximum recursion depth to prevent infinite loops (default: 10)
        current_depth: Current recursion depth (internal use)

    Returns:
        Sanitized version of the input data
    """
    import re

    # Prevent infinite recursion
    if current_depth >= max_depth:
        return f"[MAX_DEPTH_REACHED: {type(data).__name__}]"

    if data is None:
        return None

    if isinstance(data, str):
        # Remove control characters, newlines, carriage returns, and other dangerous chars
        sanitized = re.sub(r'[\r\n\t\x00-\x1f\x7f-\x9f]', '', data)

        # Limit length to prevent log flooding
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length] + "..."

        return sanitized

    elif isinstance(data, dict):
        sanitized_dict = {}
        for key, value in data.items():
            # Sanitize both key and value
            sanitized_key = sanitize_for_logging(key, max_length, max_depth, current_depth + 1)
            sanitized_value = sanitize_for_logging(value, max_length, max_depth, current_depth + 1)
            sanitized_dict[sanitized_key] = sanitized_value
        return sanitized_dict

    elif isinstance(data, (list, tuple)):
        sanitized_list = []
        for item in data:
            sanitized_item = sanitize_for_logging(item, max_length, max_depth, current_depth + 1)
            sanitized_list.append(sanitized_item)

        # Return same type as input (list or tuple)
        return type(data)(sanitized_list)

    elif isinstance(data, (int, float, bool, Decimal)):
        # Numeric and boolean types are safe as-is
        return data

    else:
        # For other types, convert to string and sanitize
        try:
            str_representation = str(data)
            return sanitize_for_logging(str_representation, max_length, max_depth, current_depth + 1)
        except Exception:
            return f"[NON_SERIALIZABLE: {type(data).__name__}]"
