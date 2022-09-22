#!/bin/bash
sudo yum -y install jq bash-completion
pip install --upgrade --force-reinstall botocore boto3 awscli

AWS_DEFAULT_REGION=$(curl -s 169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region)
echo $AWS_DEFAULT_REGION
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
echo $AWS_ACCOUNT_ID
IMAGE_TAG=latest
IMAGE_REPO_NAME=abalone

git config --global credential.helper '!aws codecommit credential-helper $@'
git config --global credential.UseHttpPath true
cd ~/environment

git clone https://git-codecommit.${AWS_DEFAULT_REGION}.amazonaws.com/v1/repos/$IMAGE_REPO_NAME

echo Creating ETL branch
cd ~/environment/$IMAGE_REPO_NAME
git checkout -b etl
cp ~/environment/mlops-workshop/etl/* .
git add -A 
git commit -m "Initial commit of etl assets"
git push --set-upstream origin etl

echo Creating Main branch
cd ~/environment/$IMAGE_REPO_NAME
git checkout -b main
git branch --unset-upstream
git rm -rf .
cp -R ~/environment/mlops-workshop/model/* .
sed -i "s/<Region>/${AWS_DEFAULT_REGION}/" ~/environment/$IMAGE_REPO_NAME/Dockerfile
sed -i "s/<AccountId>/${AWS_ACCOUNT_ID}/" ~/environment/$IMAGE_REPO_NAME/trainingjob.json
sed -i "s/<Region>/${AWS_DEFAULT_REGION}/" ~/environment/$IMAGE_REPO_NAME/trainingjob.json
sed -i "s/<AccountId>/${AWS_ACCOUNT_ID}/" ~/environment/$IMAGE_REPO_NAME/evaluationJob.json
git add -A 
git commit -m "Initial commit of model assets" 
git push --set-upstream origin main
aws codecommit update-default-branch --repository-name $IMAGE_REPO_NAME --default-branch-name main

echo Creating Test branch
cd ~/environment/$IMAGE_REPO_NAME
git checkout -b test
git rm -rf .
cp -R ~/environment/mlops-workshop/tests/system_test/* .
mkdir unit_test
cp -R ~/environment/mlops-workshop/tests/unit_test/* ./unit_test
git add -A
git commit -m "Initial commit of system test assets"
git push --set-upstream origin test

echo Creating Deploy branch
cd ~/environment/$IMAGE_REPO_NAME
git checkout -b deploy
git rm -rf .
cp -R ~/environment/mlops-workshop/deploy/* .
git add -A
git commit -m "Initial commit of deploy assets"
git push --set-upstream origin deploy

echo Done





