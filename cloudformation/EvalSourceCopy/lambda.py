import boto3
import io
import zipfile
import json
import os
import logging

s3 = boto3.client('s3')
cp = boto3.client('codepipeline')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)
    try:
        file_name = "model/evaluation.py"
        executionId=""
        pipeline_name=os.environ['PIPELINE_NAME']
        model_name = os.environ['MODEL_NAME']
        jobId = event['CodePipeline.job']['id']
        accountId = event['CodePipeline.job']['accountId']
        role = os.environ['ROLE']
        output_bucket = os.environ['PIPELINE_BUCKET']
        response = cp.get_pipeline_state(name=pipeline_name)
        for stageState in response['stageStates']:
            if stageState['stageName'] == 'CopySourceCode':
                for actionState in stageState['actionStates']:
                    if actionState['actionName'] == 'EvalSource':
                        executionId = stageState['latestExecution']['pipelineExecutionId']
        for inputArtifacts in event["CodePipeline.job"]["data"]["inputArtifacts"]:
            print(inputArtifacts)
            if inputArtifacts['name'] == 'ModelSourceOutput':
                s3Location = inputArtifacts['location']['s3Location']
                zip_bytes = s3.get_object(Bucket=s3Location['bucketName'], Key=s3Location['objectKey'])['Body'].read()
                with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as z:
                   eval_script = z.read(file_name).decode('ascii')
                   s3_upload_path = executionId + "/input/evaluation/code/" + file_name
                   s3.put_object(Bucket=output_bucket, Body=eval_script, Key=s3_upload_path)
        cp.put_job_success_result(jobId=jobId)
    except Exception as e:
        logger.error(e)
        cp.put_job_failure_result(
            jobId=jobId,
            failureDetails={
                'type': 'ConfigurationError',
                'message': str(e),
                'externalExecutionId': context.aws_request_id
            }
        )
    return 'Done'