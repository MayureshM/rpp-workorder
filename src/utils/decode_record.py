import base64
from decimal import Decimal

import simplejson as json
from dynamodb_json import json_util
from rpp_lib.logs import LOGGER


def decode_record(record):
    decoded_record = None

    try:
        decoded_record = json.loads(
            json.dumps(
                json_util.loads(
                    json.loads(
                        base64.b64decode(record["kinesis"]["data"]), parse_float=Decimal
                    )
                )
            ),
            parse_float=Decimal,
        )

    except UnicodeDecodeError as exc:
        message = "Invalid stream data, ignoring"
        record = record
        reason = str(exc)
        exception = exc
        response = "N/A"

        LOGGER.warning(
            {
                "event": message,
                "record": record,
                "reason": reason,
                "exception": exception,
                "response": response,
            }
        )

    return decoded_record
