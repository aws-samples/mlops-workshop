import boto3
import botocore
import io
import zipfile
import json
import os
import logging

s3 = boto3.client('s3')
cp = boto3.client('codepipeline')
sm = boto3.client("sagemaker")
cw = boto3.client('events')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)
    model_arn = None
    try:
        executionId=None
        pipelineExecutionSteps = None
        pipelineExecution = None
        pipeline_name=os.environ['PIPELINE_NAME']
        role=os.environ['ROLE']
        endpointName=os.environ['ENDPOINT']
        model_name = os.environ['MODEL_NAME']
        jobId = event['CodePipeline.job']['id']
        response = cp.get_pipeline_state(name=pipeline_name)
        for stageState in response['stageStates']:
            if stageState['stageName'] == 'PipelineExecution':
                for actionState in stageState['actionStates']:
                    if actionState['actionName'] == 'SubmitPipeline':
                        executionId = stageState['latestExecution']['pipelineExecutionId']
        actionExecutionDetails = cp.list_action_executions(
                                pipelineName=pipeline_name,
                                filter={
                                        'pipelineExecutionId': executionId
                                        },
                                )
        for actionDetail in actionExecutionDetails['actionExecutionDetails']:
            if actionDetail['actionName'] == 'SubmitPipeline':
                pipelineExecutionSteps = sm.list_pipeline_execution_steps(
                    PipelineExecutionArn=actionDetail['output']['outputVariables']['PipelineExecutionArn']
                )
                for pipelineExecutionStep in pipelineExecutionSteps['PipelineExecutionSteps']:
                    if pipelineExecutionStep['StepName'] == 'Register':
                        model_arn = pipelineExecutionStep['Metadata']['RegisterModel']['Arn']
                pipelineExecution = sm.describe_pipeline_execution(
                        PipelineExecutionArn=actionDetail['output']['outputVariables']['PipelineExecutionArn']
                )
                sm_model_name = "Model-{}".format(pipelineExecution['PipelineExperimentConfig']['TrialName'])
                model = sm.create_model(
                     ModelName=sm_model_name,
                    PrimaryContainer={
                        'ModelPackageName': model_arn,
                    },
                    ExecutionRoleArn=role
                )
                endpointConfigName = "EndpointConfig-{}".format(pipelineExecution['PipelineExperimentConfig']['TrialName'])
                endpointConfig = sm.create_endpoint_config(
                        EndpointConfigName=endpointConfigName,
                        ProductionVariants=[
                                    {
                                    'VariantName': 'variant-name-1',
                                    'ModelName': sm_model_name,
                                    'InitialVariantWeight': 1,
                                    'ServerlessConfig': {
                                        'MemorySizeInMB': 1024,
                                        'MaxConcurrency': 20
                                    }
                                },
                            ],
                        )
                response = create_or_update_endpoint(
                                endpointName,
                                endpointConfigName
                            )
            cw.enable_rule(Name="endpoint-monitor-{}".format(model_name))
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

def create_or_update_endpoint(endpointName,
                              endpointConfigName):
    endpoint=None
    try:
        endpoint = sm.describe_endpoint(
            EndpointName=endpointName
        )
        if endpoint:
            logger.info("Existing endpoint found.. Updating endpoint...")
            endpoint = sm.update_endpoint(
                        EndpointName=endpointName,
                        EndpointConfigName=endpointConfigName
                    )
    except botocore.exceptions.ClientError as e:
        print(f"Endpoint not found.. Creating....: {e}")
        endpoint = sm.create_endpoint(
                        EndpointName=endpointName,
                        EndpointConfigName=endpointConfigName
                    )
    return endpoint
    