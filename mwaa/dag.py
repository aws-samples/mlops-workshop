from datetime import timedelta

from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator



from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.sagemaker_training import SageMakerTrainingOperator
from airflow.providers.amazon.aws.operators.sagemaker_model import SageMakerModelOperator
from airflow.providers.amazon.aws.operators.sagemaker_endpoint_config import SageMakerEndpointConfigOperator
from airflow.providers.amazon.aws.operators.sagemaker_endpoint import SageMakerEndpointOperator
from airflow.providers.amazon.aws.operators.sagemaker_processing import SageMakerProcessingOperator

import boto3
import json
import os
import shortuuid

data_bucket = os.environ.get("AIRFLOW__JOBENV__DATA_BUCKET_NAME")
output_bucket = os.environ.get("AIRFLOW__JOBENV__OUTPUT_BUCKET_NAME")
mlops_role_name = os.environ.get("AIRFLOW__JOBENV__MLOPS_ROLE")

account_id = boto3.client('sts').get_caller_identity().get('Account')
aws_region = boto3.session.Session().region_name

model_name = os.environ.get("AIRFLOW__JOBENV__MODEL_NAME")

model_quality_rmse_threshold = 3.2 # this is our business metric for model quality
model_quality_rmse_threshold = 3.0

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['user@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'mlops',
    default_args=default_args,
    description='MlOps tutorial DAG',
    # schedule_interval="*/5 * * * *",
    #schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
    catchup=False,
) as dag:
    
    def check_skip_config(**kwargs):
        print(f"Running with boto3 version {boto3.__version__}")
        should_skip_etl = kwargs['dag_run'].conf.get('skip_etl') == "true"
        print(f"Skip ETL? {should_skip_etl}")
        if should_skip_etl:
            return "get_last_dataset_id"
        return "assign_dataset_id"
    
    check_skip_config = BranchPythonOperator(
        task_id="check_skip_config",
        python_callable=check_skip_config
    )
    
    def assign_dataset_id():
        dataset_id = str(shortuuid.uuid())
        print(f"Assigning dataset_id: {dataset_id}")
        return str(dataset_id)
    
    assign_dataset_id = PythonOperator(
        task_id="assign_dataset_id",
        python_callable=assign_dataset_id
    )
    
    create_glue_job_args = {
        "GlueVersion": "3.0"
    }

    etl = AwsGlueJobOperator(
        task_id="etl",
        job_name="abalone-preprocess",  
        script_location=f"s3://{output_bucket}/code/preprocess.py",
        s3_bucket=output_bucket,
        script_args={
                '--S3_INPUT_BUCKET': data_bucket,
                '--S3_INPUT_KEY_PREFIX': 'input/raw',
                # '--S3_UPLOADS_KEY_PREFIX': 'input/uploads',
                '--S3_OUTPUT_BUCKET': output_bucket,
                '--S3_OUTPUT_KEY_PREFIX': str(assign_dataset_id.output) +'/input/data'
            },
        iam_role_name=mlops_role_name,
        retry_limit=2,
        concurrent_run_limit=3,
        create_job_kwargs=create_glue_job_args,
        dag=dag) 
        
    def store_dataset_id(**kwargs):
        dataset_id = kwargs["dataset_id"]
        print(f"Storing dataset_id: {dataset_id}")
        Variable.set(
            key="last_dataset_id",
            value=dataset_id
        )
        
    store_dataset_id = PythonOperator(
        task_id="store_dataset_id",
        python_callable=store_dataset_id,
        op_kwargs={
            "dataset_id": str(assign_dataset_id.output)
        }
    )
        
    def get_last_dataset_id(**kwargs):
        dataset_id = Variable.get(
            key="last_dataset_id",
            deserialize_json=True,
            default_var="NONE")
        return dataset_id
        
    get_last_dataset_id = PythonOperator(
        task_id="get_last_dataset_id",
        python_callable=get_last_dataset_id,
        op_kwargs={
            "last_dataset_id": str(etl.output)
        })
    
    def get_model_digest():
        ecr = boto3.client('ecr')
        response = ecr.list_images(
            repositoryName=model_name,
            filter={
                'tagStatus': "TAGGED"
            }
        )   
        images = response["imageIds"]
        latest_images = [image for image in images if image.get("imageTag") == "latest"]
        
        if len(latest_images) == 1:
            image_id = latest_images[0]["imageDigest"].strip("sha256:")[0:16]
        else:
            image_id = None
            
        print(image_id)
        return image_id 
        
    get_model_digest = PythonOperator(
        task_id="get_model_digest",
        python_callable=get_model_digest,
        trigger_rule = TriggerRule.ONE_SUCCESS
    )

    training_job_name = f"{model_name}-{Variable.get(key='last_dataset_id', default_var='NONE')}-{str(get_model_digest.output)}"

    output_path = f"s3://{output_bucket}/{model_name}"

    model_artifact_path = f"{output_path}/{training_job_name}/output/model.tar.gz"

    mlops_role_arn = f"arn:aws:iam::{account_id}:role/{mlops_role_name}"

    training_image = f"{account_id}.dkr.ecr.{aws_region}.amazonaws.com/abalone:latest"

    train_config = {
                "AlgorithmSpecification":{
                    "TrainingImage": training_image,
                    "TrainingInputMode": "File"
                },
                "RoleArn": mlops_role_arn,
                "OutputDataConfig": {
                    "S3OutputPath": output_path
                },
                "ResourceConfig": {
                    "InstanceCount": 1,
                    "InstanceType": "ml.m5.xlarge",
                    "VolumeSizeInGB": 30
                },
                "TrainingJobName": training_job_name,
                "HyperParameters": {
                    "epochs": "2000",
                    "layers": "2",
                    "dense_layer": "64",
                    "batch_size": "8"
                },
                "StoppingCondition": {
                    "MaxRuntimeInSeconds": 360000
                },
                "InputDataConfig": [
                    {
                        "ChannelName": "training",
                        "DataSource": {
                            "S3DataSource": {
                                "S3DataType": "S3Prefix",
                                "S3Uri": f"s3://{output_bucket}/{Variable.get(key='last_dataset_id', default_var='NONE')}/input/data/training/",
                                "S3DataDistributionType": "FullyReplicated"
                            }
                        },
                        "ContentType": "text/csv",
                        "CompressionType": "None"
                    }
                ],
                "Tags": []
            }
    
    train_op = SageMakerTrainingOperator(
        task_id='model_training',
        config=train_config,
        wait_for_completion=True,
        dag=dag)
        
    model_image = f"{account_id}.dkr.ecr.{aws_region}.amazonaws.com/{model_name}:latest"
    model_data_url = f"s3://{output_bucket}/{model_name}/{training_job_name}/output/model.tar.gz"
        
    create_model = SageMakerModelOperator(
        task_id="create_model",
        config={
            "ModelName": training_job_name,
            "ExecutionRoleArn": f"arn:aws:iam::{account_id}:role/{mlops_role_name}",
            "PrimaryContainer": {
                "Image": model_image,
                "ModelDataUrl": model_data_url
            }
        }
    )
    
    def ensure_model_package_group(model_group_name, model_group_description):
        client = boto3.client("sagemaker")
        try:
            response = client.create_model_package_group(
                ModelPackageGroupName = model_group_name, 
                ModelPackageGroupDescription = model_group_description
            )
        except Exception as e:
            if "already exists" in str(e):
                print("Model Package Group already exists")
            else:
                raise e
                
    create_model_package_group = PythonOperator(
        task_id="create_model_package_group",
        python_callable=ensure_model_package_group,
        op_kwargs={
            "model_group_name": "abalone",
            "model_group_description": "Predict abalone age from external measurements"
        }
    )

    def register_model_version(container_arn, model_location):
        client = boto3.client("sagemaker")
        modelpackage_inference_specification = {
            "InferenceSpecification": {
                "Containers": [
                    {
                        "Image": container_arn
                    }
                ],
                "SupportedContentTypes": [ "text/csv" ],
                "SupportedResponseMIMETypes": [ "text/csv" ]
            }
        }

        model_url = model_location
        modelpackage_inference_specification["InferenceSpecification"]["Containers"][0]["ModelDataUrl"] = model_url

        create_model_package_input_dict = {
            "ModelPackageGroupName": "abalone",
            "ModelPackageDescription": "Predict abalone age based on external measurements",
            "ModelApprovalStatus": "PendingManualApproval"
        }

        create_model_package_input_dict.update(modelpackage_inference_specification)

        create_model_package_response = client.create_model_package(**create_model_package_input_dict)

        return create_model_package_response["ModelPackageArn"]

    register_model = PythonOperator(
        task_id="register_model",
        python_callable=register_model_version,
        op_kwargs={
            "container_arn": training_image,
            "model_location": model_data_url,
        }
    )

    testing_data = f"s3://{output_bucket}/{str(assign_dataset_id.output)}/input/data/testing/test.csv"

    model_metrics_path = f"{output_path}/{training_job_name}/"

    testing_data = f"s3://{output_bucket}/{str(assign_dataset_id.output)}/input/data/testing/test.csv"

    model_metrics_path = f"{output_path}/{training_job_name}/"

    evaluate_job_config = {
       "ProcessingJobName": f"{training_job_name}-evaluate",
       "AppSpecification":{
          "ImageUri": training_image,
          "ContainerEntrypoint":["python", "evaluation.py"]
       },
       "Environment":{
          "Stage":"Evaluation"
       },
       "ProcessingInputs":[
          {
             "InputName":"TestingData",
             "S3Input":{
                "LocalPath":"/opt/ml/processing/input/testing",
                "S3Uri": testing_data,
                "S3DataDistributionType":"FullyReplicated",
                "S3DataType":"S3Prefix",
                "S3InputMode":"File",
                "S3CompressionType":"None"
             }
          },
        {
             "InputName":"model",
             "S3Input":{
                "LocalPath":"/opt/ml/processing/input/model",
                "S3Uri": model_artifact_path,
                "S3DataDistributionType":"FullyReplicated",
                "S3DataType":"S3Prefix",
                "S3InputMode":"File",
                "S3CompressionType":"None"
        }
          }
       ],
       "ProcessingOutputConfig":{
          "Outputs":[
             {
                "OutputName":"evaluation",
                "S3Output":{
                   "LocalPath":"/opt/ml/processing/output/evaluation",
                   "S3Uri": model_metrics_path,
                   "S3UploadMode":"EndOfJob"
                }
             }
          ]
       },
       "ProcessingResources":{
          "ClusterConfig":{
             "InstanceCount":1,
             "InstanceType":"ml.m5.xlarge",
             "VolumeSizeInGB":30
          }
       },
       "RoleArn": mlops_role_arn,
       "StoppingCondition":{
          "MaxRuntimeInSeconds":86400
       },
       "Tags": []
    }

    evaluate_model = SageMakerProcessingOperator(
        task_id="evaluate_model",
        config=evaluate_job_config,
        wait_for_completion=True,
        print_log=True,
        action_if_job_exists="increment",
        aws_conn_id=None,
    )

    def check_evaluation_metrics(bucket, model_name, training_job_name):
        s3_client = boto3.client("s3")
        response = s3_client.get_object(
            Bucket=bucket,
            Key=f"{model_name}/{training_job_name}/evaluation.json"
        )
        data = json.load(response["Body"])
        rmse = data["regression_metrics"]["rmse"]["value"]
        result = rmse < model_quality_rmse_threshold
        print(f"{model_name} training job {training_job_name} RMSE: {rmse} < {model_quality_rmse_threshold}? {str(result)}")
        return result


    check_evaluation_metrics = ShortCircuitOperator(
        task_id="check_evaluation_metrics",
        python_callable=check_evaluation_metrics,
        op_kwargs={
            "bucket": output_bucket,
            "model_name": model_name,
            "training_job_name": training_job_name
        }
    )

    endpoint_config_name = f"abalone-dev-{Variable.get(key='last_dataset_id', default_var='NONE')}-{str(get_model_digest.output)}"
        
    dev_endpoint_config = SageMakerEndpointConfigOperator(
        task_id="create_dev_model_config",
        config={
            "EndpointConfigName": endpoint_config_name,
            "ProductionVariants": [
                {
                    "InstanceType": "ml.t2.large",
                    "InitialInstanceCount": 1,
                    "InitialVariantWeight": 1.0,
                    "ModelName": training_job_name,
                    "VariantName": "AllTraffic"
                }
            ]
        }
    )
    
    deploy_dev = SageMakerEndpointOperator(
        task_id="deploy_dev",
        config={
            # "EndpointName": f"abalone-dev-{Variable.get(key='last_dataset_id')}-{str(get_model_digest.output)}",
            "EndpointName": "abalone-dev",
            "EndpointConfigName": endpoint_config_name,
            "DeploymentConfig": {
                "BlueGreenUpdatePolicy": {
                    "TrafficRoutingConfiguration": {
                        "Type": "ALL_AT_ONCE",
                        "WaitIntervalInSeconds": 300
                    }
                }
            }
        }
    )
    
    check_skip_config >> [get_last_dataset_id, assign_dataset_id]
    assign_dataset_id.set_downstream(etl)
    etl.set_downstream(store_dataset_id)
    store_dataset_id.set_downstream(get_model_digest)
    get_last_dataset_id.set_downstream(get_model_digest)
    get_model_digest.set_downstream(train_op)
    train_op.set_downstream(evaluate_model)
    evaluate_model.set_downstream(check_evaluation_metrics)
    check_evaluation_metrics.set_downstream(create_model_package_group)
    create_model_package_group.set_downstream(create_model)
    create_model.set_downstream(register_model)
    register_model.set_downstream(dev_endpoint_config)
    dev_endpoint_config.set_downstream(deploy_dev)

