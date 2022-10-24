import boto3
import json

def register_model_version(container_arn, model_location):
    client = boto3.client("sagemaker")
    modelpackage_inference_specification = {
        "InferenceSpecification": {
            "Containers": [
                {
                    "Image": container_arn
                }
            ],
            "SupportedContentTypes": ["text/csv"],
            "SupportedResponseMIMETypes": ["text/csv"]
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

def get_train_config(account_id, aws_region, mlops_role_arn, output_bucket, model_name, dataset_id, model_digest):
    training_job_name = f"{model_name}-{str(dataset_id)}-{model_digest}"
    return {
                "AlgorithmSpecification":{
                    "TrainingImage": f"{account_id}.dkr.ecr.{aws_region}.amazonaws.com/{model_name}:latest",
                    "TrainingInputMode": "File"
                },
                "RoleArn": mlops_role_arn,
                "OutputDataConfig": {
                    "S3OutputPath": f"s3://{output_bucket}/{model_name}"
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
                                "S3Uri": f"s3://{output_bucket}/{dataset_id}/input/data/training/",
                                "S3DataDistributionType": "FullyReplicated"
                            }
                        },
                        "ContentType": "text/csv",
                        "CompressionType": "None"
                    }
                ],
                "Tags": []
            }

def get_evaluation_config(training_job_name, training_image, testing_data, model_s3_artifacts, mlops_role_arn, output_bucket, model_name):
    return {
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
             "InputName": "TestingData",
             "S3Input": {
                "LocalPath": "/opt/ml/processing/input/testing",
                "S3Uri": testing_data,
                "S3DataDistributionType": "FullyReplicated",
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
                "S3CompressionType": "None"
             }
          },
        {
             "InputName":"model",
             "S3Input":{
                "LocalPath": "/opt/ml/processing/input/model",
                "S3Uri": model_s3_artifacts,
                "S3DataDistributionType": "FullyReplicated",
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
                "S3CompressionType": "None"
        }
          }
       ],
       "ProcessingOutputConfig":{
          "Outputs":[
             {
                "OutputName": "evaluation",
                "S3Output": {
                   "LocalPath": "/opt/ml/processing/output/evaluation",
                   "S3Uri": f"s3://{output_bucket}/{model_name}/{training_job_name}/",
                   "S3UploadMode": "EndOfJob"
                }
             }
          ]
       },
       "ProcessingResources": {
          "ClusterConfig": {
             "InstanceCount": 1,
             "InstanceType": "ml.m5.xlarge",
             "VolumeSizeInGB": 30
          }
       },
       "RoleArn": mlops_role_arn,
       "StoppingCondition": {
          "MaxRuntimeInSeconds": 86400
       },
       "Tags": []
    }

def check_evaluation_metrics(bucket, model_name, training_job_name, model_quality_threshold):
    s3_client = boto3.client("s3")
    response = s3_client.get_object(
        Bucket=bucket,
        Key=f"{model_name}/{training_job_name}/evaluation.json"
    )
    data = json.load(response["Body"])
    rmse = data["regression_metrics"]["rmse"]["value"]
    result = rmse < model_quality_threshold
    print(f"{model_name} training job {training_job_name} RMSE: {rmse} < {model_quality_threshold}? {str(result)}")
    return result

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