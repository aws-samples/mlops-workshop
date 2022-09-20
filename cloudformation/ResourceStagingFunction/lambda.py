import json
import boto3
import cfnresponse

client = boto3.client("s3")


def lambda_handler(event, context):
    event_type = event["RequestType"]
    print(f"The event is: {event_type}")

    response_data = {}

    target_bucket = event['ResourceProperties']["TargetBucket"]
    target_key = event['ResourceProperties']["TargetKey"]

    try:
        if event_type in ('Create', 'Update'):
            print(f"Writing requirements.txt to s3://{target_bucket}/{target_key}")

            file_contents = '''shortuuid
boto3>=1.20
'''

            response = client.put_object(
                Body=file_contents,
                Bucket=target_bucket,
                Key=target_key
            )
            print(f"Got response: {response}")
            print("Object created")
        elif event_type == "Delete":
            print("Deleting s3://{target_bucket}/{target_key}")

            s3 = boto3.resource('s3')
            bucket = s3.Bucket(target_bucket)
            response = bucket.object_versions.filter(Prefix=target_key).delete()
            print(f"Got response: {response}")
            print("Object deleted")
        print("Operation successful")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
    except Exception as e:
        print("Operation Failed")
        print(str(e))
        response_data["Data"] = str(e)
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data)

    return {
        'statusCode': 200,
        'body': json.dumps('Resource Staged')
    }

