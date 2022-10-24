from datetime import timedelta

from airflow import DAG

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
import os


from operators.ecr import get_model_digest
from operators.sagemaker import register_model_version, get_train_config, get_evaluation_config, check_evaluation_metrics, ensure_model_package_group
from operators.util import check_skip_config, assign_dataset_id

data_bucket = os.environ.get("AIRFLOW__JOBENV__DATA_BUCKET_NAME")
output_bucket = os.environ.get("AIRFLOW__JOBENV__OUTPUT_BUCKET_NAME")
mlops_role_name = os.environ.get("AIRFLOW__JOBENV__MLOPS_ROLE")
account_id = os.environ.get("AIRFLOW__JOBENV__AWS_ACCOUNT_ID")
aws_region = os.environ.get("AIRFLOW__JOBENV__AWS_REGION")

mlops_role_arn = f"arn:aws:iam::{account_id}:role/{mlops_role_name}"

model_name = os.environ.get("AIRFLOW__JOBENV__MODEL_NAME")

model_quality_rmse_threshold = 3.2 # this is our business metric for model quality

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
    start_date=days_ago(1),
    tags=['example'],
    catchup=False,
) as dag:
    
    check_skip_config = BranchPythonOperator(
        task_id="check_skip_config",
        python_callable=check_skip_config
    )
    
    assign_dataset_id = PythonOperator(
        task_id="assign_dataset_id",
        python_callable=assign_dataset_id,
        op_kwargs = {
            "model_name": "abalone",
        }
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
                '--S3_OUTPUT_BUCKET': output_bucket,
                '--S3_OUTPUT_KEY_PREFIX': str(assign_dataset_id.output) +'/input/data'
            },
        iam_role_name=mlops_role_name,
        retry_limit=2,
        concurrent_run_limit=3,
        create_job_kwargs=create_glue_job_args,
        dag=dag)
        
    get_model_digest = PythonOperator(
        task_id="get_model_digest",
        python_callable=get_model_digest,
        trigger_rule = TriggerRule.ONE_SUCCESS,
        op_kwargs = {
            "model_name": "abalone",
        }
    )

    train_op = SageMakerTrainingOperator(
        task_id='model_training',
        config=get_train_config(
            account_id,
            aws_region,
            mlops_role_arn,
            output_bucket,
            model_name,
            str(assign_dataset_id.output),
            str(get_model_digest.output)),
        wait_for_completion=True,
        dag=dag)
        
    create_model = SageMakerModelOperator(
        task_id="create_model",
        config={
            "ModelName": model_name,
            "ExecutionRoleArn": f"arn:aws:iam::{account_id}:role/{mlops_role_name}",
            "PrimaryContainer": {
                "Image": f"{account_id}.dkr.ecr.{aws_region}.amazonaws.com/abalone:latest",
                "ModelDataUrl": f"s3://mlops-{aws_region}-{account_id}/{model_name}/{model_name}-{str(assign_dataset_id.output)}-{str(get_model_digest.output)}/output/model.tar.gz"
            }
        }
    )
                
    create_model_package_group = PythonOperator(
        task_id="create_model_package_group",
        python_callable=ensure_model_package_group,
        op_kwargs={
            "model_group_name": "abalone",
            "model_group_description": "Predict abalone age from external measurements"
        }
    )

    register_model = PythonOperator(
        task_id="register_model",
        python_callable=register_model_version,
        op_kwargs={
            "container_arn": f"{account_id}.dkr.ecr.{aws_region}.amazonaws.com/abalone:latest",
            "model_location": f"s3://mlops-{aws_region}-{account_id}/{model_name}/{model_name}-{str(assign_dataset_id.output)}-{str(get_model_digest.output)}/output/model.tar.gz",
        }
    )

    evaluate_model = SageMakerProcessingOperator(
        task_id="evaluate_model",
        config=get_evaluation_config(
            f"{model_name}-{str(assign_dataset_id.output)}-{str(get_model_digest.output)}",
            f"{account_id}.dkr.ecr.{aws_region}.amazonaws.com/abalone:latest",
            #s3: // mlops - us - east - 1 - 200608916304 / DWwBmWVXJgm7FQeyhHELc3 / input / data / testing / test.csv
            f"s3://mlops-{aws_region}-{account_id}/{str(assign_dataset_id.output)}/input/data/testing", # todo: this wants to be the testing data
            f"s3://mlops-{aws_region}-{account_id}/{model_name}/{model_name}-{str(assign_dataset_id.output)}-{str(get_model_digest.output)}/output/model.tar.gz",
            mlops_role_arn,
            output_bucket,
            model_name),
        wait_for_completion=True,
        print_log=True,
        action_if_job_exists="increment",
        aws_conn_id=None,
    )

    check_evaluation_metrics = ShortCircuitOperator(
        task_id="check_evaluation_metrics",
        python_callable=check_evaluation_metrics,
        op_kwargs={
            "bucket": output_bucket,
            "model_name": model_name,
            "training_job_name": f"{model_name}-{str(assign_dataset_id.output)}-{str(get_model_digest.output)}",
            "model_quality_threshold": model_quality_rmse_threshold
        }
    )

    endpoint_config_name = f"abalone-dev-{str(assign_dataset_id.output)}-{str(get_model_digest.output)}"
        
    dev_endpoint_config = SageMakerEndpointConfigOperator(
        task_id="create_dev_model_config",
        config={
            "EndpointConfigName": endpoint_config_name,
            "ProductionVariants": [
                {
                    "InstanceType": "ml.t2.large",
                    "InitialInstanceCount": 1,
                    "InitialVariantWeight": 1.0,
                    # "ModelName": f"{model_name}-{str(assign_dataset_id.output)}-{str(get_model_digest.output)}",
                    "ModelName": "abalone",
                    "VariantName": "AllTraffic"
                }
            ]
        }
    )
    
    deploy_dev = SageMakerEndpointOperator(
        task_id="deploy_dev",
        config={
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
    
    check_skip_config.set_downstream([get_model_digest, assign_dataset_id])
    assign_dataset_id.set_downstream(etl)
    etl.set_downstream(get_model_digest)
    get_model_digest.set_downstream(train_op)
    train_op.set_downstream(evaluate_model)
    evaluate_model.set_downstream(check_evaluation_metrics)
    check_evaluation_metrics.set_downstream(create_model_package_group)
    create_model_package_group.set_downstream(create_model)
    create_model.set_downstream(register_model)
    register_model.set_downstream(dev_endpoint_config)
    dev_endpoint_config.set_downstream(deploy_dev)