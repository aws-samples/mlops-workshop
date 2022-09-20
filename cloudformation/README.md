This project uses the Serverless Application Model, SAM, to manage its AWS resources. 
The AWS Serverless Application Model (SAM) is an open-source framework for 
building serverless applications. It provides shorthand syntax to express 
functions, APIs, databases, and event source mappings. 

You can build and deploy it in your environment as follows.

Build:
$ sam build -put mlops-mwaa.yaml 

Deploy:
$ sam deploy --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --guided

Rebuild & Update:
$ sam build -put mlops-mwaa.yaml 
$ sam deploy --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND

The project includes an Amazon Managed Workflows for Apache Airflow (MWAA) environment, 
Amazon Simple Storage Service (S3) Buckets, and AWS Lambda Functions that provide the
storage, stage etl scripts, the model repo with cicd, and trigger the MWAA Airflow Dag. 