## MLOps Workshop - [Operationalizing the Machine Learning Pipeline](http://operational-machine-learning-pipeline.workshop.aws/)

### Introduction
Data Scientists and ML Practitioners need more than just a Jupyter notebook to build, test, and deploy ML models into production. They also need to ensure they perform these tasks in a reliable, flexible, and automated fashion.

There are three basic questions that should be considered when starting the ML journey to develop a model for a real Business Case:

- How long would it take your organization to deploy a change that involves a single line of code?

- How do you account for model concept drift in production?

- Can you build, test and deploy models in a repeatable, reliable and automated way?

So, if you’re not happy with your answers to these questions, then MLOps is a concept that can help you to:

- Create or improve the organization culture and agility for Continuous Integration/Continuous Delivery (CI/CD).

- Create an automated infrastructure that will support your processes to operationalize your ML model.

### What is MLOps?
MLOps stands for Machine Learning Operations. MLOps focuses on the intersection of data science, and data engineering, in combination with existing DevOps practices, to streamline ML model delivery across the machine learning development lifecycle.

Why do we need MLOps?
Machine learning operations are key to effectively transitioning from an experimentation phase, to a production phase. The practice provides you the ability to create a repeatable mechanism to build, train, deploy, and manage machine learning models. Adopting MLOps practices gives you faster time-to-market for ML projects, by delivering the following benefits.

- **Productivity:** Providing self-service environments with access to curated data sets, lets data engineers, and data scientists waste less time with missing or invalid data.  
- **Repeatability:** Automating all the steps in the ML ML model building, and ML model deployments, are automated without becomming a bottleneck down the road.  
- **Reliability:** Incorporating CI/CD practices allows ML models to be deployed quickly, while enuring quality, and consistency.  
- **Auditability:** Provide end-to-end traceability. This includes additional considerations, that are unique to machine learning, such as versioning all inputs and outputs, from data science experiments, to source data, to trained model. This means that we can demonstrate exactly how the ML model was built, and where it was deployed.  
- **Data and model quality:** MLOps lets us enforce policies that guard against ML model bias, and track changes to the data's statistical properties, and model quality over time.

### Getting Started
By following along with the **[Operationalizing the Machine Learning Pipeline](http://operational-machine-learning-pipeline.workshop.aws/)** AWS workshop, the branches in this repository represent different approaches you can take to MLOps, You can create a workflow centered around the Data Scientist or ML Engineer with the `sagemaker-pipeline` branch. Alternativley, should you wish to explore a code centric approach, the `dev-tools` branch show how the AWS Developer Tools can be used to manage CICD processes around the source code, as well as the orchestration of data ETL, and other model building processes.
Furthermore, should you want to build a more data centric workflow, the `mwaa` branch builds the model training workflow using Amazon Managed Workflows for Apache Airflow (MWAA). Teams supporting a diverse set of workloads often prefer to standardize on a set of automation tools and leverage AWS developer tools to manage their models.  
  
After you have selected an approach, you can clone the branch and deploy a lab environment using the Makefile found in the cloudformation/ directory.

```bash
git clone https://github.com/aws-samples/mlops-workshop.git
cd mlops-workshop/cloudformation
make deploy
```

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

