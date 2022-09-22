#!/bin/bash
sudo yum -y install jq bash-completion

AWS_DEFAULT_REGION=$(curl -s 169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)
echo $AWS_DEFAULT_REGION
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
echo $AWS_ACCOUNT_ID
IMAGE_TAG=latest
IMAGE_REPO_NAME=abalone

cd ~/environment/mlops-workshop/model
pwd
if test -f Dockerfile; then
    sed -i "s/<Region>/${AWS_DEFAULT_REGION}/" Dockerfile
else
    echo "DockerFile not found!"
    exit 0
fi

echo Build started on `date`
echo Logging in to the Amazon Deep Learning Contain Repository ...
aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin 763104351884.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com
echo Building the Container image...
docker build -t $IMAGE_REPO_NAME:$IMAGE_TAG .
echo Tagging to repository
docker tag $IMAGE_REPO_NAME:$IMAGE_TAG $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com/$IMAGE_REPO_NAME:$IMAGE_TAG
echo Logging in to ECR Repository...
$(aws ecr get-login --no-include-email --region $AWS_DEFAULT_REGION)
echo Pushing the Container image...
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com/$IMAGE_REPO_NAME:$IMAGE_TAG
echo done
