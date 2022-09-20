import os
import io
import json
import logging
import boto3
import time
import botocore
import numpy as np
import pandas as pd
from sklearn import preprocessing
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")
smr = boto3.client("sagemaker-runtime")
cp = boto3.client("codepipeline")
cf = boto3.resource("cloudformation")

def test_endpoint(bucket, key, endpoint_name):
    """
    Description:
    ------------
    Executes model predictions on the testing dataset.
    
    :bucket: (str) Pipeline S3 Bucket.
    :key: (str) Path to "testing" dataset.
    :endpoint_name: (str) Name of the 'Dev' endpoint to test.

    :returns: Lists of ground truth, prediction labels and response times.
    
    """
    column_names = ["rings", "length", "diameter", "height", "whole weight", "shucked weight",
                    "viscera weight", "shell weight", "sex_F", "sex_I", "sex_M"]
    response_times = []
    predictions = []
    y_test = []
    obj = s3.get_object(Bucket=bucket, Key=key)
    test_df = pd.read_csv(io.BytesIO(obj['Body'].read()), names=column_names)
    y = test_df['rings'].to_numpy()
    X = test_df.drop(['rings'], axis=1).to_numpy()
    X = preprocessing.normalize(X)
    
    # Cycle through each row of the data to get a prediction
    for row in range(len(X)):
        payload = ",".join(map(str, X[row]))
        elapsed_time = time.time()
        try:
            response = smr.invoke_endpoint(
                EndpointName=endpoint_name,
                ContentType = "text/csv",
                Body=payload
            )
        except ClientError as e:
            error_message = e.response["Error"]["Message"]
            logger.error(error_message)
            raise Exception(error_message)
        response_times.append(time.time() - elapsed_time)
        result = np.asarray(response['Body'].read().decode('utf-8').rstrip('\n'))
        predictions.append(float(result))
        y_test.append(float(y[row]))
    
    return y_test, predictions, response_times


def handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)


    # Get Input Variables    
    pipeline_name = os.environ['PIPELINE_NAME']
    bucket = os.environ['PIPELINE_BUCKET']
    endpoint_name = os.environ['ENDPOINT']
    test_data = os.environ['TEST_DATA']
    jobId = event['CodePipeline.job']['id']
    key = None
    executionId = None 

    
    response = cp.get_pipeline_state(name=pipeline_name)
    for stageState in response['stageStates']:
        if stageState['stageName'] == 'DeployDev':
            for actionState in stageState['actionStates']:
                if actionState['actionName'] == 'DeployDevModel':
                    executionId = stageState['latestExecution']['pipelineExecutionId']
    key =  os.path.join(executionId, test_data)
    logger.info(key)
    
    # Get the evaluation results from SageMaker hosted model
    logger.info("Evaluating SageMaker Endpoint ...")
    times = test_endpoint(bucket, key, endpoint_name)

    # Save Metrics to S3 for Model Package
    logger.info("Average Endpoint Response Time: {:.2f}s".format(np.mean(times)))
    cp.put_job_success_result(jobId=jobId)
    # Return results
    logger.info("Done!")
    return {
        "statusCode": 200,
        "AvgResponseTime": "{:.2f} seconds".format(np.mean(times))
    }