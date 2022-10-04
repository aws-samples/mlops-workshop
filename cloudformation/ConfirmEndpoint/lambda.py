import boto3
import io
import os
import logging
from botocore.exceptions import ClientError

sm = boto3.client('sagemaker')
cw = boto3.client('events')
cp = boto3.client('codepipeline')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)
    pipeline_name = os.environ['PIPELINE_NAME']
    model_name = os.environ['MODEL_NAME']
    endpoint_name = os.environ['ENDPOINT']
    token = None
    executionId = None
    try:
        response = cp.get_pipeline_state(name=pipeline_name)
        for stageState in response['stageStates']:
            if stageState['stageName'] == 'DeployEndpoint':
                for actionState in stageState['actionStates']:
                    if actionState['actionName'] == 'ConfirmEndpoint':
                        latestExecution = actionState['latestExecution']
                        executionId = stageState['latestExecution']['pipelineExecutionId']
                        if latestExecution['status'] != 'InProgress':
                            raise(Exception("Step is not in progress: {}".format(latestExecution['status'])))
                        token = latestExecution['token']
        if token is None:
            raise(Exception("Action token wasn't found. Aborting..."))
        response = sm.describe_endpoint(
            EndpointName=endpoint_name
        )
        status = response['EndpointStatus']
        logger.info(status)
        if status == "InService":
            result = {
                'summary': 'Endpoint created successfully',
                'status': 'Approved'
            }
        elif (status == "Creating" or status =="Updating" or status == "SystemUpdating"):
            return "Endpoint creation Job ({}) in progress".format(executionId)
        else:
            result = {
                'summary': response['FailureReason'],
                'status': 'Rejected'
            }

    except Exception as e:
        result = {
            'summary': str(e),
            'status': 'Rejected'
        }

    try:
        response = cp.put_approval_result(
            pipelineName=pipeline_name,
            stageName='DeployEndpoint',
            actionName='ConfirmEndpoint',
            result=result,token=token
        )
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)
    
    try:
        response = cw.disable_rule(Name="endpoint-monitor-{}".format(model_name))
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)
    
    return "Done!"
