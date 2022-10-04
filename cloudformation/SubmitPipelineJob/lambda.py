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
    try:
        mlops_sm_pipeline_defn = "mlops_sm_pipeline_defn.json"
        mlops_sm_pipeline_param = "sagemaker_pipeline_params.json"
        pipeline_defn=None
        pipeline_params=None
        executionId=None
        region = os.environ['AWS_REGION']
        role = os.environ['ROLE']
        cp_pipeline_name=os.environ['PIPELINE_NAME']
        model_name = os.environ['MODEL_NAME']
        output_bucket = os.environ['PIPELINE_BUCKET']
        jobId = event['CodePipeline.job']['id']
        accountId = event['CodePipeline.job']['accountId']
        response = cp.get_pipeline_state(name=cp_pipeline_name)
        for stageState in response['stageStates']:
            if stageState['stageName'] == 'PipelineExecution':
                for actionState in stageState['actionStates']:
                    if actionState['actionName'] == 'SubmitPipeline':
                        executionId = stageState['latestExecution']['pipelineExecutionId']
        for inputArtifacts in event["CodePipeline.job"]["data"]["inputArtifacts"]:
            if inputArtifacts['name'] == 'ModelSourceOutput':
                s3Location = inputArtifacts['location']['s3Location']
                zip_bytes = s3.get_object(Bucket=s3Location['bucketName'], Key=s3Location['objectKey'])['Body'].read()
                with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as z:
                    pipeline_defn = json.loads(z.read(mlops_sm_pipeline_defn).decode('ascii'))
                    pipeline_params = json.loads(z.read(mlops_sm_pipeline_param).decode('ascii'))
        if pipeline_defn is None:
            raise(Exception("mlops-sm-mlops_sm_pipeline_defn.json not found"))
        if pipeline_params is None:
            raise(Exception("sagemaker_pipeline_params.json not found"))    
        json_pipeline = update_pipeline_definition (pipeline_defn, 
                                                    role,
                                                    accountId,
                                                    output_bucket,
                                                    executionId)
        response = create_or_update_pipeline(
                                  json_pipeline,
                                  pipeline_params,
                                  role)
        pipeline_execution_arn = submit_pipeline(pipeline_params, accountId)
        cw.enable_rule(Name="pipeline-monitor-{}".format(model_name))
        cp.put_job_success_result(jobId=jobId, 
                                  outputVariables = {
                                    'PipelineExecutionArn': pipeline_execution_arn['PipelineExecutionArn']
                                    }
                                )
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

def submit_pipeline(pipeline_params, accountId):
    response = sm.start_pipeline_execution(
            PipelineName=pipeline_params['Parameters']['PipelineName'],
            PipelineParameters=[
                {
                    'Name': 'ProcessingInstanceCount',
                    'Value': pipeline_params['Parameters']['ProcessingInstanceCount']
                    
                },
                {
                    'Name': 'TrainingInstanceCount',
                    'Value': pipeline_params['Parameters']['TrainingInstanceCount'],
                },
                {
                    'Name': 'ModelApprovalStatus',
                    'Value': pipeline_params['Parameters']['ModelApprovalStatus']
                },
                {    
                    'Name': 'MseThreshold',
                    'Value': pipeline_params['Parameters']['MseThreshold']
                    
                },
                {
                    'Name': 'mlops_bucket',
                    'Value': pipeline_params['Parameters']['mlops_bucket'].replace('<Region>', os.environ['AWS_REGION']).replace('<AccountId>', accountId)
                },
                {
                    'Name': 'data_bucket',
                    'Value': pipeline_params['Parameters']['data_bucket'].replace('<Region>', os.environ['AWS_REGION']).replace('<AccountId>', accountId)
                    
                }
                
            ],
            PipelineExecutionDescription='Pipeline instance for Abalone model',
            ParallelismConfiguration={
                'MaxParallelExecutionSteps': pipeline_params['Parameters']['MaxParallelExecutionSteps']
            }
        )
    return response
    

def create_or_update_pipeline(json_pipeline,
                              pipeline_params,
                              role):
    pipeline_arn=None
    try:
        pipeline_arn = sm.describe_pipeline(
            PipelineName=pipeline_params['Parameters']['PipelineName']
        )
        if pipeline_arn:
            logger.info("Existing pipeline found.. Updating Pipeline...")
            sm.update_pipeline(
            PipelineName=pipeline_params['Parameters']['PipelineName'],
            PipelineDefinition=json.dumps(json_pipeline),
            RoleArn=role,
            ParallelismConfiguration={
                'MaxParallelExecutionSteps': pipeline_params['Parameters']['MaxParallelExecutionSteps']
                }
            )
    except botocore.exceptions.ClientError as e:
        print(f"Exception in pipelines.run_pipeline:main: {e}")
        pipeline_arn = sm.create_pipeline(
                        PipelineName=pipeline_params['Parameters']['PipelineName'],
                        PipelineDefinition=json.dumps(json_pipeline),
                        RoleArn=role,
                        ParallelismConfiguration={
                            'MaxParallelExecutionSteps': pipeline_params['Parameters']['MaxParallelExecutionSteps']
                            }
                        )
    return pipeline_arn


def update_pipeline_definition(pipeline, 
                    role,
                    accountId,
                    output_bucket,
                    executionId):
    for step in pipeline['Steps']:
        if step['Name'] == 'ETL':
            #Update Region
            image_uri = step['Arguments']['AppSpecification']['ImageUri'].replace('<Region>', os.environ['AWS_REGION'])
            step['Arguments']['AppSpecification']['ImageUri'] = image_uri
            
            ##Update Role
            step['Arguments']['RoleArn'] = role
            
            for processinginput in step['Arguments']['ProcessingInputs']:
                if processinginput['InputName'] == 'code':
                    #Update location for preprocessing.py
                    etl_code_location = "s3://{}/{}/{}".format(output_bucket, executionId, "input/etl/code/preprocessing.py")
                    processinginput['S3Input']['S3Uri'] = etl_code_location

        if step['Name'] == 'Train':
            #Update Region and AccountId
            training_image = step['Arguments']['AlgorithmSpecification']['TrainingImage'].replace('<Region>', os.environ['AWS_REGION']).replace('<AccountId>', accountId)
            step['Arguments']['AlgorithmSpecification']['TrainingImage'] = training_image
            
            ##Update Role
            step['Arguments']['RoleArn'] = role
            
            #Update Region and AccountId
            rule_evaluator_image = step['Arguments']['ProfilerRuleConfigurations'][0]['RuleEvaluatorImage'].replace('<Region>', os.environ['AWS_REGION'])
            step['Arguments']['ProfilerRuleConfigurations'][0]['RuleEvaluatorImage'] = rule_evaluator_image
        
        if step['Name'] == 'Evaluate':
            #Update Region
            image_uri = step['Arguments']['AppSpecification']['ImageUri'].replace('<Region>', os.environ['AWS_REGION']).replace('<AccountId>', accountId)
            step['Arguments']['AppSpecification']['ImageUri'] = image_uri
            
            ##Update Role
            step['Arguments']['RoleArn'] = role
            
            for processinginput in step['Arguments']['ProcessingInputs']:
                if processinginput['InputName'] == 'code':
                    #Update location for evaluation.py
                    eval_code_location = "s3://{}/{}/{}".format(output_bucket, executionId, "input/evaluation/code/evaluation.py")
                    processinginput['S3Input']['S3Uri'] = eval_code_location
        
        if step['Name'] == 'Condition':
            for ifStep in step['Arguments']['IfSteps']:
                #Update Evaluation Image
                eval_image = ifStep['Arguments']['InferenceSpecification']['Containers'][0]['Image'].replace('<Region>', os.environ['AWS_REGION']).replace('<AccountId>', accountId)
                ifStep['Arguments']['InferenceSpecification']['Containers'][0]['Image'] = eval_image
    return pipeline