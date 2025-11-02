import json
import boto3
import logging
from botocore.exceptions import ClientError
from environs import Env

# Initialize environment variables and clients
env = Env()
env.read_env()
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(env('RPPReconWorkOrderTable'))
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    for record in event['Records']:
        event_name = record['eventName']
        try:
            if event_name in ['INSERT', 'MODIFY']:
                new_image = record['dynamodb']['NewImage']
                item = {k: list(v.values())[0] for k, v in new_image.items()}
                table.put_item(Item=item)
                logger.info(f"Successfully added/modified item: {item}")
            elif event_name == 'REMOVE':
                keys = record['dynamodb']['Keys']
                key = {k: list(v.values())[0] for k, v in keys.items()}
                table.delete_item(Key=key)
                logger.info(f"Successfully deleted item with key: {key}")
        except ClientError as e:
            logger.error(f"Failed to process item: {e.response['Error']['Message']}")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
