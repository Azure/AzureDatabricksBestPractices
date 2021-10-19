# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Azure ML Deployment
# MAGIC 
# MAGIC Azure ML is an Azure managed service that allows for model serving via a REST endpoint.  This lesson is designed to be a template for getting a proof of concept working on Azure ML.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Train a basic model
# MAGIC * Create or load an Azure ML workspace
# MAGIC * Create an image for the model deployment
# MAGIC * Deploy the image to ACI
# MAGIC * Deploy the image to AKS
# MAGIC * Update the model and clean up the deployment
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Some of the details of configuring Azure are left up to the user and will depend on your specific Azure configuration.  **This notebook cannot be run as-is.** 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Azure ML
# MAGIC 
# MAGIC Azure offers a way to deploy machine learning models named Azure ML.  It allows data scientists a way of deploying machine learning models, offering a REST endpoint to make inference calls to.  MLflow integrates with Azure ML by way of images using Azure Machine Learning service workspaces.  In order to use Azure ML you therefore need resources and permissions to these Azure services.

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
experimentPath = "/Users/" + username + "/AzureML"
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

# MAGIC %md
# MAGIC ### Create or load an Azure ML Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Ensure you are using or create a cluster specifying 
# MAGIC   * **Databricks Runtime Version:** Databricks Runtime 5.2 or above
# MAGIC   * **Python Version:** Python 3
# MAGIC 1. Install required libraries through the Clusters tab on the left-hand side of the screen:
# MAGIC    1. Create required libraries.
# MAGIC       * Source **PyPI** and enter `mlflow`.
# MAGIC       * Source **PyPI** and enter `azureml-sdk[databricks]`.
# MAGIC    1. Install the libraries into the cluster.
# MAGIC 1. Attach this notebook to the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC Before models can be deployed to Azure ML, you must create or obtain an Azure ML Workspace. The `azureml.core.Workspace.create()` function will load a workspace of a specified name or create one if it does not already exist. For more information about creating an Azure ML Workspace, see the [Azure ML Workspace management documentation](https://docs.microsoft.com/en-us/azure/machine-learning/service/how-to-manage-workspace).

# COMMAND ----------

import azureml
from azureml.core import Workspace

workspace_name = ""
workspace_location = ""
resource_group = ""
subscription_id = ""

workspace = Workspace.create(name = workspace_name,
                             location = workspace_location,
                             resource_group = resource_group,
                             subscription_id = subscription_id,
                             exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building an Azure Container Image for model deployment
# MAGIC 
# MAGIC Use the `mlflow.azuereml.build_image` function to build an Azure Container Image for the trained MLflow model. This function also registers the MLflow model with a specified Azure ML workspace. The resulting image can be deployed to Azure Container Instances (ACI) or Azure Kubernetes Service (AKS) for real-time serving.

# COMMAND ----------

import mlflow.azureml

# Build the image
model_image, azure_model = mlflow.azureml.build_image(model_path=modelPath, 
                                                      workspace=workspace, 
                                                      run_id=runID,
                                                      model_name="model",
                                                      image_name="model",
                                                      description="Sklearn random forest image for predicting airbnb housing prices",
                                                      synchronous=False)

# Wait for the image creation
model_image.wait_for_creation(show_output=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying the model to "dev"
# MAGIC 
# MAGIC The [Azure Container Instances (ACI) platform](https://docs.microsoft.com/en-us/azure/container-instances/) is the recommended environment for staging and developmental model deployments.
# MAGIC 
# MAGIC Using the Azure ML SDK, deploy the Container Image for the trained MLflow model to ACI.  This involves creating a webservice deployment

# COMMAND ----------

from azureml.core.webservice import AciWebservice, Webservice

# Create the deploymnet 
dev_webservice_name = "airbnb-model"
dev_webservice_deployment_config = AciWebservice.deploy_configuration()
dev_webservice = Webservice.deploy_from_image(name=dev_webservice_name, 
                                              image=model_image, 
                                              deployment_config=dev_webservice_deployment_config, 
                                              workspace=workspace)

# Wait for the image deployment
dev_webservice.wait_for_deployment()

# COMMAND ----------

# MAGIC %md
# MAGIC Query the deployed model in "dev".  First create a query input.

# COMMAND ----------

sample = df.drop(["price"], axis=1).iloc[[0]]

query_input = sample.to_json(orient='split')
query_input = eval(query_input)
query_input.pop('index', None)

query_input

# COMMAND ----------

# MAGIC %md
# MAGIC Create a wrapper function to do the querying.

# COMMAND ----------

import requests
import json

def query_endpoint_example(scoring_uri, inputs, service_key=None):
  headers = {
    "Content-Type": "application/json",
  }
  if service_key is not None:
    headers["Authorization"] = "Bearer {service_key}".format(service_key=service_key)
    
  print("Sending batch prediction request with inputs: {}".format(inputs))
  response = requests.post(scoring_uri, data=json.dumps(inputs), headers=headers)
  preds = json.loads(response.text)
  print("Received response: {}".format(preds))
  return preds

# COMMAND ----------

# MAGIC %md
# MAGIC Query the ACI endpoint.

# COMMAND ----------

dev_prediction = query_endpoint_example(scoring_uri=dev_webservice.scoring_uri, inputs=query_input)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying the model using AKS.
# MAGIC 
# MAGIC [Azure Kubernetes Service (AKS)](https://azure.microsoft.com/en-us/services/kubernetes-service/) is the preferred option for production model deployment. Do Option 1 or Option 2.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a new AKS cluster
# MAGIC 
# MAGIC If you do not have an active AKS cluster for model deployment, create one using the Azure ML SDK.

# COMMAND ----------

from azureml.core.compute import AksCompute, ComputeTarget

# Use the default configuration (you can also provide parameters to customize this)
prov_config = AksCompute.provisioning_configuration()

aks_cluster_name = "diabetes-cluster" 
# Create the cluster
aks_target = ComputeTarget.create(workspace = workspace, 
                                  name = aks_cluster_name, 
                                  provisioning_configuration = prov_config)

# Wait for the create process to complete
aks_target.wait_for_completion(show_output = True)
print(aks_target.provisioning_state)
print(aks_target.provisioning_errors)

# COMMAND ----------

# MAGIC %md
# MAGIC If you already have an active AKS cluster running, you can add it to your Workspace using the Azure ML SDK.

# COMMAND ----------

# from azureml.core.compute import AksCompute, ComputeTarget

# # Get the resource id from https://porta..azure.com -> Find your resource group -> click on the Kubernetes service -> Properties
# resource_id = "/subscriptions/<subscription-id>/resourcegroups/<resource-group>/providers/Microsoft.ContainerService/managedClusters/<aks-service-name>"

# # Give the cluster a local name
# cluster_name = "<cluster-name>"

# # Attatch the cluster to your workgroup
# aks_target = AksCompute.attach(workspace=workspace, name=cluster_name, resource_id=resource_id)

# # Wait for the operation to complete
# aks_target.wait_for_completion(True)
# print(aks_target.provisioning_state)
# print(aks_target.provisioning_errors)

# COMMAND ----------

# MAGIC %md
# MAGIC Deploy to the model's image to the specified AKS cluster

# COMMAND ----------

from azureml.core.webservice import Webservice, AksWebservice

# Set configuration and service name
prod_webservice_name = "diabetes-model-prod"
prod_webservice_deployment_config = AksWebservice.deploy_configuration()

# Deploy from image
prod_webservice = Webservice.deploy_from_image(workspace = workspace, 
                                               name = prod_webservice_name,
                                               image = model_image,
                                               deployment_config = prod_webservice_deployment_config,
                                               deployment_target = aks_target)

# Wait for the deployment to complete
prod_webservice.wait_for_deployment(show_output = True)

# COMMAND ----------

# MAGIC %md
# MAGIC Query the AKS webservice's scoring endpoint by sending an HTTP POST request that includes the input vector. The production AKS deployment may require an authorization token (service key) for queries. Include this key in the HTTP request header.

# COMMAND ----------

import requests
import json

def query_endpoint_example(scoring_uri, inputs, service_key=None):
  headers = {
    "Content-Type": "application/json",
  }
  if service_key is not None:
    headers["Authorization"] = "Bearer {service_key}".format(service_key=service_key)
    
  print("Sending batch prediction request with inputs: {}".format(inputs))
  response = requests.post(scoring_uri, data=json.dumps(inputs), headers=headers)
  preds = json.loads(response.text)
  print("Received response: {}".format(preds))
  return preds

# COMMAND ----------

# MAGIC %md
# MAGIC Pull the endpoing URI and key.

# COMMAND ----------

prod_scoring_uri = prod_webservice.scoring_uri
prod_service_key = prod_webservice.get_keys()[0] if len(prod_webservice.get_keys()) > 0 else None

# COMMAND ----------

# MAGIC %md
# MAGIC Query the endpoint.

# COMMAND ----------

prod_prediction1 = query_endpoint_example(scoring_uri=prod_scoring_uri, service_key=prod_service_key, inputs=query_input)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Updating the production deployment

# COMMAND ----------

# MAGIC %md
# MAGIC Build an Azure Container Image for a new model

# COMMAND ----------

run_id2 = "<run-id2>"

# COMMAND ----------

import mlflow.azureml

model_image_updated, azure_model_updated = mlflow.azureml.build_image(model_path="model", 
                                                                      workspace=workspace, 
                                                                      run_id=run_id2,
                                                                      model_name="model-updated",
                                                                      image_name="model-updated",
                                                                      description="Sklearn ElasticNet image for predicting airbnb housing",
                                                                      synchronous=False)

model_image_updated.wait_for_creation(show_output=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Using the [`azureml.core.webservice.AksWebservice.update()`](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.webservice.akswebservice?view=azure-ml-py#update) function, replace the deployment's existing model image with the new model image.

# COMMAND ----------

prod_webservice.update(image=model_image_updated)

prod_webservice.wait_for_deployment(show_output = True)

# COMMAND ----------

# MAGIC %md
# MAGIC Query the updated model

# COMMAND ----------

prod_prediction2 = query_endpoint_example(scoring_uri=prod_scoring_uri, service_key=prod_service_key, inputs=query_input)

# COMMAND ----------

# MAGIC %md
# MAGIC Compare the predictions

# COMMAND ----------

print("Run ID: {} Prediction: {}".format(run_id1, prod_prediction1)) 
print("Run ID: {} Prediction: {}".format(run_id2, prod_prediction2))

# COMMAND ----------

# MAGIC %md
# MAGIC Clean up the deployments

# COMMAND ----------

# MAGIC %md
# MAGIC Because ACI manages compute resources on your behalf, deleting the "dev" ACI webservice will remove all resources associated with the "dev" model deployment

# COMMAND ----------

dev_webservice.delete()

# COMMAND ----------

# MAGIC %md
# MAGIC This terminates the real-time serving webservice running on the specified AKS cluster. It **does not** terminate the AKS cluster.

# COMMAND ----------

prod_webservice.delete()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove the AKS cluster from the Azure ML Workspace.
# MAGIC 
# MAGIC If the cluster was created using the Azure ML SDK, remove it from the Azure ML Workspace will terminate the cluster, including all of its compute resources and deployments.
# MAGIC 
# MAGIC If the cluster was created independently, it will remain active after removal from the Azure ML Workspace.

# COMMAND ----------

aks_target.delete()


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>