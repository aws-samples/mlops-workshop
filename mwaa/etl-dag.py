from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from airflow.utils.dates import days_ago

import boto3
import json
import io
import os
import uuid
import time

# todo: automate env vars from cfn stack
#data_bucket = "data-us-west-2-200608916304"
data_bucket = os.environ.get("AIRFLOW__JOBENV__DATA_BUCKET_NAME")
#output_bucket = "mlops-us-west-2-200608916304"
output_bucket = os.environ.get("AIRFLOW__JOBENV__OUTPUT_BUCKET_NAME")
#executionId = "1"
#glue_job_name = "abalone-preprocess-4e12e111-6591-4a5b-8a10-9f396b723e2c"
mlops_role_arn = os.environ.get("AIRFLOW__JOBENV__MLOPS_ROLE")

account_id = boto3.client('sts').get_caller_identity().get('Account')

model_name = os.environ.get("AIRFLOW__JOBENV__MODEL_NAME")

execution_id = str(uuid.uuid1())

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['famestad@amazon.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'mlops',
    default_args=default_args,
    description='MlOps tutorial DAG',
    schedule_interval="*/5 * * * *",
    #schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
    catchup=False,
) as dag:

    t0_s3_trigger = S3PrefixSensor(
        task_id="WatchDataBucket",
        bucket_name=data_bucket,
        prefix="input/uploads",
    )

    def monitor_etl(job_name, job_run_id, glue):
        print("Monitoring job status")
        response = glue.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response['JobRun']['JobRunState']
        while status in ["RUNNING", "RUNNING"]:
            print("Waiting for job: {}".format(job_run_id))
            time.sleep(5)
            response = glue.get_job_run(JobName=job_name, RunId=job_run_id)
            status = response['JobRun']['JobRunState']

        print("Status: {}".format(status))
        if status == "SUCCEEDED":
            result = {
                'summary': 'Glue ETL Job completed',
                'status': 'Approved',
                'execution_id': execution_id
            }
        elif status == "RUNNING":
            return "Glue ETL Job ({}) is in progress".format(job_run_id)
        elif status == "STARTING":
            return "Glue ETL Job ({}) is in progress".format(job_run_id)
        else:
            result = {
                'summary': response['JobRun']['ErrorMessage'],
                'status': 'Rejected',
                'execution_id': execution_id
            }
        return result

    def launch_etl():
        for k, v in os.environ.items():
            print(f'{k}={v}')
        glue = boto3.client('glue')
        job_name = "abalone-preprocess"
        script_location = "s3://{}/code/preprocess.py".format(output_bucket)
        
        print("Using script_location: {}".format(script_location))
        print(f"Execution ID: {execution_id}")
        
        try:
            glue.get_job(JobName=job_name)
        except Exception as err:
            print(f"Got exception: {err}")
            print(f"No existing job found named {job_name}")
            etlJob = {}
            etlJob['Name'] = job_name
            etlJob['Role'] = mlops_role_arn
            etlJob["Command"] = {
                "Name": "pythonshell",
                "ScriptLocation": script_location,
                "PythonVersion": "3"
            }
            print(f"Creating ETL Job: ${job_name}")
            # glue_job_name = glue.create_job(**etlJob)['Name']
        
        print(f"Starting ETL Job: ${job_name}")
        print(f"Execution ID: {execution_id}")
        job_run_id = glue.start_job_run(
            JobName=job_name,
            Arguments={
                '--S3_INPUT_BUCKET': data_bucket,
                '--S3_INPUT_KEY_PREFIX': 'input/raw',
                '--S3_UPLOADS_KEY_PREFIX': 'input/uploads',
                '--S3_OUTPUT_BUCKET': output_bucket,
                '--S3_OUTPUT_KEY_PREFIX': execution_id +'/input'
            }
        )
        print(job_run_id)

        response = glue.get_job_runs(JobName=job_name)
        job_run_id = response['JobRuns'][0]['Id']

        result = monitor_etl(job_name, job_run_id, glue)
        
        if not result["status"] == "Approved":
            raise Exception(f"Job Failed with result {result}")

        return result

    t1_etl = PythonOperator(
        task_id="LaunchETL",
        python_callable=launch_etl,
    )
    
    def monitor_training_job(job_name, sm):
        while True:
            time.sleep(5)
            response = sm.describe_training_job(TrainingJobName=job_name)
            print(f"Training job status: {response['TrainingJobStatus']}")
            if not response['TrainingJobStatus'] == "InProgress":
                return response['TrainingJobStatus']

    def train_model(**kwargs):
        ti = kwargs["ti"]
        execution_id = ti.xcom_pull(task_ids="LaunchETL")["execution_id"]
        print(execution_id)
        print(f"Execution ID: {execution_id}")
        sm = boto3.client('sagemaker')
        pipeline_bucket = f"mlops-us-west-2-{account_id}"
        training_job = {
                "AlgorithmSpecification":{
                    "TrainingImage": f"{account_id}.dkr.ecr.us-west-2.amazonaws.com/abalone:latest",
                    "TrainingInputMode": "File"
                },
                "RoleArn": f"arn:aws:iam::{account_id}:role/MLOps",
                "OutputDataConfig": {
                    "S3OutputPath": ""
                },
                "ResourceConfig": {
                    "InstanceCount": 1,
                    "InstanceType": "ml.m5.xlarge",
                    "VolumeSizeInGB": 30
                },
                "TrainingJobName": "",
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
                                "S3Uri": "",
                                "S3DataDistributionType": "FullyReplicated"
                            }
                        },
                        "ContentType": "text/csv",
                        "CompressionType": "None"
                    }
                ],
                "Tags": []
            }
        
        job_name = "mlops-{}-{}".format(model_name, execution_id)
        training_job['TrainingJobName'] = job_name
        training_job['OutputDataConfig']['S3OutputPath'] = os.path.join('s3://', pipeline_bucket, execution_id)
        training_job['InputDataConfig'][0]['DataSource']['S3DataSource']['S3Uri'] = os.path.join('s3://', pipeline_bucket, execution_id, 'input/training')
        #training_job['Tags'].append({'Key': 'jobid', 'Value': jobId})

        response = sm.create_training_job(**training_job)
        print(response)
        result = monitor_training_job(job_name, sm)
        if not result == "Completed":
            raise Exception(f"Training job failed with result: {result}")
        return result

    t2_train_model = PythonOperator(
        task_id="TrainModel",
        python_callable=train_model,
    )

    # TODO: Is this the right place for the rest? Should we be back in CICD?

    # def deploy_dev():
    #     pass
    #
    # t3_deploy_dev = PythonOperator(
    #     task_id="DeployDev",
    #     python_callable=deploy_dev,
    # )
    #
    # def system_test():
    #     pass
    #
    # t4_system_test = PythonOperator(
    #     task_id="SystemTest",
    #     python_callable=system_test,
    # )
    #
    # def deploy_prod():
    #     pass
    #
    # t5_deploy_prod = PythonOperator(
    #     task_id="DeployProd",
    #     python_callable=deploy_prod,
    # )

    # t1 >> [t2, t3]
    t0_s3_trigger >> t1_etl >> t2_train_model # >> t3_deploy_dev >> t4_system_test >> t5_deploy_prod

# TODO: Reimplement below pipeline as an airflow dag

#
# on source or data change
# "build application", stage scripts - todo details
# run etl and create train/ test splits
# train model
# deploy dev
# system test
# deploy prod

#  MLOpsPipeline:
#    Type: AWS::CodePipeline::Pipeline
#    Properties:
#      Name: !Sub ${AWS::StackName}
#      RoleArn: !Sub arn:aws:iam::${AWS::AccountId}:role/${RoleName}
#      ArtifactStore:
#          Type: S3
#          Location: !Sub mlops-${AWS::Region}-${AWS::AccountId}
#      Stages:
#        - Name: Source
#          Actions:
#            - Name: ModelSource
#              ActionTypeId:
#                Category: Source
#                Owner: AWS
#                Version: "1"
#                Provider: CodeCommit
#              OutputArtifacts:
#                - Name: ModelSourceOutput
#              Configuration:
#                BranchName: master
#                RepositoryName: mlops
#                PollForSourceChanges: true
#              RunOrder: 1
#            - Name: ETLSource
#              ActionTypeId:
#                Category: Source
#                Owner: AWS
#                Version: "1"
#                Provider: CodeCommit
#              OutputArtifacts:
#                - Name: EtlSourceOutput
#              Configuration:
#                BranchName: etl
#                RepositoryName: mlops
#                PollForSourceChanges: true
#              RunOrder: 1
#            - Name: DataSource
#              ActionTypeId:
#                Category: Source
#                Owner: AWS
#                Version: "1"
#                Provider: S3
#              OutputArtifacts:
#                - Name: DataSourceOutput
#              Configuration:
#                S3Bucket: !Sub data-${AWS::Region}-${AWS::AccountId}
#                S3ObjectKey: input/raw/abalone.csv
#                PollForSourceChanges: true
#              RunOrder: 1
#            - Name: TestSource
#              ActionTypeId:
#                Category: Source
#                Owner: AWS
#                Version: "1"
#                Provider: CodeCommit
#              OutputArtifacts:
#                - Name: TestSourceOutput
#              Configuration:
#                BranchName: test
#                RepositoryName: mlops
#                PollForSourceChanges: true
#              RunOrder: 1
#        - Name: Build
#          Actions:
#            - Name: BuildImage
#              InputArtifacts:
#                - Name: ModelSourceOutput
#              OutputArtifacts:
#                - Name: BuildImageOutput
#              ActionTypeId:
#                Category: Build
#                Owner: AWS
#                Version: "1"
#                Provider: CodeBuild
#              Configuration:
#                ProjectName: !Ref BuildImageProject
#              RunOrder: 1
#        - Name: ETL
#          Actions:
#            - Name: GlueJob
#              InputArtifacts:
#                - Name: EtlSourceOutput
#              ActionTypeId:
#                Category: Invoke
#                Owner: AWS
#                Version: "1"
#                Provider: Lambda
#              Configuration:
#                FunctionName: !Ref EtlLaunchJob
#              RunOrder: 1
#        - Name: ETLApproval
#          Actions:
#            - Name: ApproveETL
#              ActionTypeId:
#                Category: Approval
#                Owner: AWS
#                Version: "1"
#                Provider: Manual
#              Configuration:
#                  CustomData: 'Did the Glue ETL Job run successfully?'
#              RunOrder: 1
#        - Name: Train
#          Actions:
#            - Name: TrainModel
#              InputArtifacts:
#                - Name: ModelSourceOutput
#              OutputArtifacts:
#                - Name: ModelTrainOutput
#              ActionTypeId:
#                Category: Invoke
#                Owner: AWS
#                Version: "1"
#                Provider: Lambda
#              Configuration:
#                  FunctionName: !Ref TrainingLaunchJob
#                  UserParameters: !Sub mlops-airflow-${ModelName}
#              RunOrder: 1
#        - Name: TrainApproval
#          Actions:
#            - Name: ApproveTrain
#              ActionTypeId:
#                Category: Approval
#                Owner: AWS
#                Version: "1"
#                Provider: Manual
#              Configuration:
#                  CustomData: 'Was this model trained successfully?'
#              RunOrder: 1
#        - Name: DeployDev
#          Actions:
#            - Name: BuildDevDeployment
#              InputArtifacts:
#                - Name: ModelSourceOutput
#              OutputArtifacts:
#                - Name: BuildDevOutput
#              ActionTypeId:
#                Category: Build
#                Owner: AWS
#                Version: "1"
#                Provider: CodeBuild
#              Configuration:
#                ProjectName: !Ref BuildDeploymentProject
#                EnvironmentVariables: '[{"name":"STAGE","value":"Dev","type":"PLAINTEXT"}]'
#              RunOrder: 1
#            - Name: DeployDevModel
#              InputArtifacts:
#                - Name: BuildDevOutput
#              OutputArtifacts:
#                - Name: DeployDevOutput
#              ActionTypeId:
#                Category: Deploy
#                Owner: AWS
#                Version: "1"
#                Provider: CloudFormation
#              Configuration:
#                ActionMode: REPLACE_ON_FAILURE
#                RoleArn: !Sub arn:aws:iam::${AWS::AccountId}:role/${RoleName}
#                Capabilities: CAPABILITY_NAMED_IAM
#                StackName: !Sub ${AWS::StackName}-deploy-dev
#                TemplateConfiguration: BuildDevOutput::Dev-config-export.json
#                TemplatePath: BuildDevOutput::deploy-model-Dev.yml
#              RunOrder: 2
#        - Name: SystemTest
#          Actions:
#            - Name: BuildTestingWorkflow
#              InputArtifacts:
#                - Name: TestSourceOutput
#              OutputArtifacts:
#                - Name: BuildTestingWorkflowOutput
#              ActionTypeId:
#                Category: Build
#                Owner: AWS
#                Version: "1"
#                Provider: CodeBuild
#              Configuration:
#                ProjectName: !Ref BuildWorkflowProject
#              RunOrder: 1
#            - Name: ExecuteSystemTest
#              InputArtifacts:
#                - Name: BuildTestingWorkflowOutput
#              OutputArtifacts:
#                - Name: SystemTestingOutput
#              ActionTypeId:
#                Category: Invoke
#                Owner: AWS
#                Version: "1"
#                Provider: StepFunctions
#              Configuration:
#                StateMachineArn: !Sub arn:aws:states:${AWS::Region}:${AWS::AccountId}:stateMachine:${AWS::StackName}-systemtest
#                InputType: FilePath
#                Input: input.json
#              RunOrder: 2
#        - Name: DeployPrd
#          Actions:
#            - Name: BuildPrdDeployment
#              InputArtifacts:
#                - Name: ModelSourceOutput
#              OutputArtifacts:
#                - Name: BuildPrdOutput
#              ActionTypeId:
#                Category: Build
#                Owner: AWS
#                Version: "1"
#                Provider: CodeBuild
#              Configuration:
#                ProjectName: !Ref BuildDeploymentProject
#                EnvironmentVariables: '[{"name":"STAGE","value":"Prd","type":"PLAINTEXT"}]'
#              RunOrder: 1
#            - Name: DeployPrdModel
#              InputArtifacts:
#                - Name: BuildPrdOutput
#              OutputArtifacts:
#                - Name: DeployPrdOutput
#              ActionTypeId:
#                Category: Deploy
#                Owner: AWS
#                Version: "1"
#                Provider: CloudFormation
#              Configuration:
#                ActionMode: CREATE_UPDATE
#                RoleArn: !Sub arn:aws:iam::${AWS::AccountId}:role/${RoleName}
#                Capabilities: CAPABILITY_NAMED_IAM
#                StackName: !Sub ${AWS::StackName}-deploy-prd
#                TemplateConfiguration: BuildPrdOutput::Prd-config-export.json
#                TemplatePath: BuildPrdOutput::deploy-model-Prd.yml
#              RunOrder: 2