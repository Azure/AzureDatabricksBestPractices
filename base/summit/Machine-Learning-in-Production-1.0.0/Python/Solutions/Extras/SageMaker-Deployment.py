# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # SageMaker Deployment
# MAGIC 
# MAGIC SageMaker is an AWS managed service that allows for model serving via a REST endpoint.  This lesson is designed to be a template for getting a proof of concept working on SageMaker.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Train a basic model
# MAGIC * Deploy it to SageMaker
# MAGIC * Perform REST calls against the EndPoint
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Some of the details of configuring AWS are left up to the user and will depend on your specific AWS configuration.  **This notebook cannot be run as-is.**  It requires IAM roles specific to your AWS account.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with SageMaker
# MAGIC 
# MAGIC AWS offers the managed service SageMaker.  It allows data scientists a way of deploying machine learning models, offering a REST endpoint to make inference calls to.  MLflow integrates with SageMaker by way of containers using Amazon Container Services (ACS).  In order to use SageMaker you therefore need the following:<br><br>
# MAGIC 
# MAGIC 1. IAM credentials that give access to ACS and SageMaker
# MAGIC 2. An MLflow container image on ACS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Training the Model
# MAGIC 
# MAGIC First train a model ont he `airbnb` dataset.

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

import mlflow
from mlflow.exceptions import MlflowException
from  mlflow.tracking import MlflowClient
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
import mlflow.sklearn

# Define paths
experimentPath = "/Users/" + username + "/SageMaker"
modelPath = "random-forest-model"

# Create experiment
try:
  experimentID = mlflow.create_experiment(experimentPath)
except MlflowException:
  experimentID = MlflowClient().get_experiment_by_name(experimentPath).experiment_id
  mlflow.set_experiment(experimentPath)

print("The experiment can be found at the path `{}` and has an experiment_id of `{}`".format(experimentPath, experimentID))

# Train and log model
df = pd.read_csv("/dbfs/mnt/conor-work/airbnb/airbnb-cleaned-mlflow.csv")
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

rf = RandomForestRegressor(n_estimators=1000, max_depth=20)
rf.fit(X_train, y_train)

with mlflow.start_run(experiment_id=experimentID, run_name="RF Model") as run: 
  mlflow.sklearn.log_model(rf, modelPath)
  runID = run.info.run_uuid
  
print("Run completed with ID {} and path {}".format(runID, modelPath))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Set up the `mlflow-pyfunc` Docker Image in ECR
# MAGIC 
# MAGIC During deployment, MLflow will use a specialized Docker container with resources required to load and serve the model. This container is named `mlflow-pyfunc`.
# MAGIC 
# MAGIC By default, MLflow will search for this container within your AWS Elastic Container Registry (ECR). You can build and upload this container to ECR using the
# MAGIC `mlflow.sagemaker.build_image()` function in MLflow, or `mlflow sagemaker build-and-push-container`, as it will require `docker`.
# MAGIC 
# MAGIC Alternatively, you can specify an alternative URL for this container by setting an environment variable as follows.
# MAGIC The ECR URL should look like: `{account_id}.dkr.ecr.{region}.amazonaws.com/{repo_name}:{tag}`
# MAGIC 
# MAGIC `os.environ["SAGEMAKER_DEPLOY_IMG_URL"] = "<ecr-url>"`
# MAGIC 
# MAGIC You can contact your Databricks representative for a prebuilt `mlflow-pyfunc` image URL in ECR.
# MAGIC 
# MAGIC Use MLflow's SageMaker API to deploy your trained model to SageMaker. The `mlflow.sagemaker.deploy()` function creates a SageMaker endpoint as well as all intermediate SageMaker objects required for the endpoint.
# MAGIC 
# MAGIC 
# MAGIC To do this at the command line, you can use something like the following:
# MAGIC 
# MAGIC ```
# MAGIC ACCOUNTID=""
# MAGIC REGION=us-west-2
# MAGIC 
# MAGIC mlflow sagemaker build-and-push-container
# MAGIC $(aws ecr get-login --no-include-email --region REGION)
# MAGIC docker tag mlflow-pyfunc:latest ${ACCOUNTID}.dkr.ecr.${REGION}.amazonaws.com/mlflow-pyfunc:latest
# MAGIC docker push ${ACCOUNTID}.dkr.ecr.${REGION}.amazonaws.com/mlflow-pyfunc:latest```
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This step is left up to the user since it depends on your IAM roles
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You must create a new SageMaker endpoint for each new region

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploying the Model
# MAGIC 
# MAGIC Once ECS is set up, you can use `mlflow.sagemaker.deploy` to deploy your model.  **This code depends on you filling out your AWS specification.** This includes:
# MAGIC <br><br>
# MAGIC 1. `app_name`: the name for how the app appears on SageMaker 
# MAGIC 2. `run_id`: the ID for the MLflow run 
# MAGIC 3. `model_path`: the path for the MLflow model 
# MAGIC 4. `region_name`: your prefered AWS region 
# MAGIC 5. `mode`: use `create` to make a new instance. You can also replace a pre-existing model
# MAGIC 6. `execution_role_arn`: the role ARN 
# MAGIC 7. `instance_type`: what size ec2 machine
# MAGIC 8. `image_url`: the URL for the ECR image

# COMMAND ----------

# MAGIC %md
# MAGIC The following is read-only credentials for a pre-made SageMaker endpoint.  They will not work for deploying a model.

# COMMAND ----------

# Set AWS credentials as environment variables
os.environ["AWS_ACCESS_KEY_ID"] = 'AKIAI4T2MLVBUB372FAA'
os.environ["AWS_SECRET_ACCESS_KEY"] = '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' # READ ONLY ACCESS KEYS
os.environ["AWS_DEFAULT_REGION"] = 'us-west-2'

# COMMAND ----------

# MAGIC %md
# MAGIC Fill in the following command with your region, ARN, and image URL to deploy your model.

# COMMAND ----------

# import mlflow.sagemaker as mfs
# import random

# appName = "airbnb-latest-{}".format(random.randint(1,10000))

# mfs.deploy(app_name=appName, 
#            run_id=runID, 
#            model_path=modelPath, 
#            region_name=" < FILL IN> ", 
#            mode="create", 
#            execution_role_arn=" < FILL IN> ", 
#            instance_type="ml.t2.medium",
#            image_url=" < FILL IN> " )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the Connection
# MAGIC 
# MAGIC You can test the connection using `boto3` and describing the endpoint.  This code works on a SageMaker endpoint that has already been set up.

# COMMAND ----------

appName = "airbnb-latest-0001"

# COMMAND ----------

import boto3

def check_status(appName):
  sage_client = boto3.client('sagemaker', region_name="us-west-2")
  endpoint_description = sage_client.describe_endpoint(EndpointName=appName)
  endpoint_status = endpoint_description["EndpointStatus"]
  return endpoint_status

print("Application status is: {}".format(check_status(appName)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Evaluate the Input Vector by Sending an HTTP Request
# MAGIC Query the SageMaker endpoint's REST API using the ``sagemaker-runtime`` API provided in `boto3`. 

# COMMAND ----------

query_input = X_train.iloc[[0]].values.flatten().tolist()

print("Using input vector: {}".format(query_input))

# COMMAND ----------

import json

def query_endpoint_example(appName, inputs):
  print("Sending batch prediction request with inputs: {}".format(inputs))
  client = boto3.session.Session().client("sagemaker-runtime", "us-west-2")
  
  response = client.invoke_endpoint(
      EndpointName=appName,
      Body=json.dumps(inputs),
      ContentType='application/json',
  )
  preds = response['Body'].read().decode("ascii")
  preds = json.loads(preds)
  print("Received response: {}".format(preds))
  return preds

prediction = query_endpoint_example(appName=appName, inputs=[query_input])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning up the deployment
# MAGIC 
# MAGIC When your model deployment is no longer needed, run the `mlflow.sagemaker.delete()` function to delete it.

# COMMAND ----------

# Specify the archive=False option to delete any SageMaker models and configurations
# associated with the specified application
# mfs.delete(app_name=appName, region_name="us-west-2", archive=False)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>