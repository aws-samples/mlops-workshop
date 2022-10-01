This project uses the Serverless Application Model, SAM, to manage its AWS resources. The AWS Serverless Application Model (SAM) is an open-source framework for building serverless applications. It provides shorthand syntax to express functions, APIs, databases, and event source mappings.

You can build and deploy it in your environment as follows.

- Build and Deploy: make deploy

The default arguments can be as follows
        Stack Name [sam-app]: **mlops-devtools**
        AWS Region [us-east-1]: **<Update with the region you are running the example in>**
        #Shows you resources changes to be deployed and require a 'Y' to initiate deploy
        Confirm changes before deploy [y/N]: **N**
        #SAM needs permission to be able to create roles to connect to the resources in your template
        Allow SAM CLI IAM role creation [Y/n]: **Y**
        #Preserves the state of previously provisioned resources when an operation fails
        Disable rollback [y/N]: **N**
        Save arguments to configuration file [Y/n]: **Y**
        SAM configuration file [samconfig.toml]: 
        SAM configuration environment [default]: 


- Rebuild & Update: $ make update

The project includes an Amazon Simple Storage Service (S3) Buckets, ECR repository to store the images, AWS Lambda Functions that provide the storage, stage etl scripts, CICD workflow using AWS Devtools, and triggers using EventBridge.