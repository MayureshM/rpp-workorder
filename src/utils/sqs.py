""" Module to handle local and remote SQS calls"""

import os
import json
import simplejson as s_json
import boto3
from rpp_lib.logs import LOGGER as log


def get_client():
    """
    handles client connection for local and remote calls
    :return:
    """
    if os.getenv("AWS_SAM_LOCAL"):
        client = boto3.client("sqs", endpoint_url="http://local-sqs:4576")
    else:
        client = boto3.client("sqs")

    return client


def send_message(queue_url, message, delay_seconds: int = 0):
    """
    pushing message to a given SQS queue
    :param queue_url:
    :param message:
    :return:
    """
    client = get_client()
    log.info(
        {
            "event": "sqs.publish.start",
            "queue_name": queue_url,
            "message": message,
            "delay_seconds": delay_seconds,
        }
    )

    try:
        response = client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message),
            DelaySeconds=delay_seconds,
        )
    except TypeError:
        response = client.send_message(
            QueueUrl=queue_url,
            MessageBody=s_json.dumps(message),
            DelaySeconds=delay_seconds,
        )

    log.info({"event": "sqs.publish.end", "message": response})

    return response
