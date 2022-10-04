import boto3
import io
import os
import logging
import botocore
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
    accountId = event['account']
    region = os.environ['AWS_REGION']
    pipeline_execution_arn=None
    executionId=None
    result = None
    token = None
    try:
        response = cp.get_pipeline_state(name=pipeline_name)
        for stageState in response['stageStates']:
            if stageState['stageName'] == 'PipelineExecution':
                for actionState in stageState['actionStates']:
                    if actionState['actionName'] == 'UpdatePipelineExecution':
                        latestExecution = actionState['latestExecution']
                        executionId = stageState['latestExecution']['pipelineExecutionId']
                        if latestExecution['status'] != 'InProgress':
                            raise(Exception("Pipeline is not awaiting approval: {}".format(latestExecution['status'])))
                        token = latestExecution['token']
        if token is None:
            raise(Exception("Action token wasn't found. Aborting..."))
        actionExecutionDetails = cp.list_action_executions(
                                pipelineName=pipeline_name,
                                filter={
                                        'pipelineExecutionId': executionId
                                        },
                                )
        for actionDetail in actionExecutionDetails['actionExecutionDetails']:
            if actionDetail['actionName'] == 'SubmitPipeline':
                response = sm.describe_pipeline_execution(
                    PipelineExecutionArn=actionDetail['output']['outputVariables']['PipelineExecutionArn']
                )
        status = response['PipelineExecutionStatus']
        logger.info(status)
        if status == "Succeeded":
            result = {
                'summary': 'Pipeline executed successfully',
                'status': 'Approved'
            }
        elif status == "Executing":
            return "Pipeline Execution in progress"
        elif status == "Stopping":
            return "Pipeline Execution in progress"
        elif status == "Stopped":
            result = {
                'summary': response['FailureReason'],
                'status': 'Rejected'
            }
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
            stageName='PipelineExecution',
            actionName='UpdatePipelineExecution',
            result=result,token=token
        )
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)
    try:
        response = cw.disable_rule(Name="pipeline-monitor-{}".format(model_name))
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)

    return "Done!"
                                        