import io
import json
import os
import logging
import urllib3
import boto3
import ast
import base64
import http.client
from botocore.exceptions import ClientError

mwaa_env_name = os.environ.get("MWAA_ENVIRONMENT_NAME")
dag_name = 'mlops'
mwaa_cli_command = 'dags trigger'

client = boto3.client("mwaa")


def handler(event, context):
    mwaa_cli_token = client.create_cli_token(
        Name=mwaa_env_name
    )

    conn = http.client.HTTPSConnection(mwaa_cli_token['WebServerHostname'])
    payload = "dags trigger " + dag_name
    headers = {
        'Authorization': 'Bearer ' + mwaa_cli_token['CliToken'],
        'Content-Type': 'text/plain'
    }
    conn.request("POST", "/aws_mwaa/cli", payload, headers)
    res = conn.getresponse()
    data = res.read()
    dict_str = data.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)
    return base64.b64decode(mydata['stdout'])