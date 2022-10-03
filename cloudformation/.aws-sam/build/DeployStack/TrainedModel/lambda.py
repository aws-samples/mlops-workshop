import json
import logging
import boto3
import os
from botocore.exceptions import ClientError

ssm = boto3.client('ssm')

logger = logging.getLogger()
logger.setLevel(logging.INFO)
model_package_name = os.environ['model_package_name']

def handler(event, context):
    logger.info(event)
    model_name = event['detail']['ModelPackageArn']
    try:
        response = ssm.put_parameter(
            Name=model_package_name,
            Overwrite=True,
            Value=model_name
    )
        logger.info(response)

    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)
    return "Done"