import json
import logging
import boto3
import os
import shutil
import cfnresponse
import requests

# this url is the latest from sagemaker-pipeline branch as a zipfile
URL = "https://github.com/aws-samples/mlops-workshop/zipball/sagemaker-pipeline"
tmp_file = "/tmp/mlops-workshop.zip"
source_code_file = "/tmp/abalone"
s3_key = os.path.join("lab", "abalone.zip")

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

s3_client = boto3.client("s3")

def create(bucket):
    print("Running CREATE")

    print("Download the zip from GitHub")
    r = requests.get(URL)
    with open(tmp_file, "wb") as zipfile:
        zipfile.write(r.content)

    print("Updating directory structure")
    # The zip from GitHub has an extra folder at the top level we need to remove
    shutil.unpack_archive(tmp_file, "/tmp/repo")
    folder_name = os.listdir("/tmp/repo")[0]
    shutil.make_archive(source_code_file,
                        format="zip",
                        root_dir=f"/tmp/repo/{folder_name}",
                        base_dir="."
    )

    print("uploading zip...")

    response = s3_client.upload_file(
        Filename=f"{source_code_file}.zip",
        Bucket=bucket,
        Key=s3_key,
    )

    return response

def delete(bucket, key):
    print("deleting model from s3...")
    response = s3_client.delete_object(
        Bucket=bucket,
        Key=s3_key
    )
    print(response)
    return response

def lambda_handler(event, context):
    request_type = event['RequestType']
    resource_properties = event["ResourceProperties"]
    try:
        if request_type in ["Create", "Update"]:
            response = create(
                bucket=resource_properties["Bucket"],
            )
        elif request_type == "Delete":
            response = delete(
                bucket=resource_properties["Bucket"],
                key=os.path.join("lab-code", "abalone.tar.gz")
            )
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response)
    except Exception as e:
        print("Operation Failed")
        print(str(e))
        response_data = {
            "Data": str(e)
        }
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data)
        return {
            'statusCode': 500,
            'body': json.dumps(response_data)
        }

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
