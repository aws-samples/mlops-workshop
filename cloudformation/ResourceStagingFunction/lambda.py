import json
import boto3
import cfnresponse

client = boto3.client("s3")


def lambda_handler(event, context):
    event_type = event["RequestType"]
    print(f"The event is: {event_type}")

    response_data = {}

    source_bucket = event['ResourceProperties']["SourceBucket"]
    target_bucket = event['ResourceProperties']["TargetBucket"]
    source_key = event['ResourceProperties']["SourceKey"]
    target_key = event['ResourceProperties']["TargetKey"]

    try:
        if event_type in ('Create', 'Update'):
            print(f"Copying s3://{source_bucket}/{source_key} to s3://{target_bucket}/{target_key}")
            copy_source = {
                "Bucket": source_bucket,
                "Key": source_key
            }
            response = client.copy(copy_source, target_bucket, target_key)
            print(f"Got response: {response}")
            print("Object created")
        elif event_type == "Delete":
            print("Deleting s3://{target_bucket}/{target_key}")
            response = client.delete_object(
                Bucket=target_bucket,
                Key=target_key
            )
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
        'body': json.dumps('Hello from Lambda!')
    }
