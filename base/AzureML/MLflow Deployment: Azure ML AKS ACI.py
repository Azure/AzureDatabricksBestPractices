# Databricks notebook source
# MAGIC %md ##MLflow Tutorial: Serving Models with Microsoft Azure ML
# MAGIC 
# MAGIC In this tutorial, we will use MLflow to train a model for rating wines and deploy it to Azure ML for real-time serving.
# MAGIC 
# MAGIC This guide consists of the following sections:
# MAGIC 
# MAGIC #### Setup
# MAGIC * Launch an Azure Databricks cluster
# MAGIC * Install MLflow
# MAGIC * Install the Azure ML SDK
# MAGIC * Create or load an Azure ML Workspace
# MAGIC * (Optional) Connect to an MLflow tracking server
# MAGIC 
# MAGIC #### Training a model
# MAGIC * Download training data
# MAGIC * In an MLflow run, train and save an ElasticNet model for rating wines
# MAGIC 
# MAGIC #### Building an Azure Container Image for model deployment
# MAGIC * Use MLflow to build a Container Image for the trained model
# MAGIC 
# MAGIC #### Deploying the model to "dev" using Azure Container Instances (ACI)
# MAGIC * Create an ACI webservice deployment using the model's Container Image
# MAGIC 
# MAGIC #### Querying the deployed model in "dev"
# MAGIC * Load a sample input vector from the wine dataset
# MAGIC * Evaluate the sample input vector by sending an HTTP request
# MAGIC 
# MAGIC #### Deploying the model to production using Azure Kubernetes Service (AKS)
# MAGIC * Option 1: Create a new AKS cluster
# MAGIC * Option 2: Connect to an existing AKS cluster
# MAGIC * Deploy to the model's image to the specified AKS cluster
# MAGIC 
# MAGIC #### Querying the deployed model in production
# MAGIC * Load a sample input vector from the wine dataset
# MAGIC * Evaluate the sample input vector by sending an HTTP request
# MAGIC 
# MAGIC #### Updating the production deployment
# MAGIC * Train a new model
# MAGIC * Build an Azure Container Image for the new model
# MAGIC * Deploy the new model's image to the AKS cluster
# MAGIC * Query the updated model
# MAGIC 
# MAGIC #### Cleaning up the deployments
# MAGIC * Terminate the "dev" ACI webservice
# MAGIC * Terminate the production AKS webservice
# MAGIC * Remove the AKS cluster from the Azure ML Workspace

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %md ### Launch an Azure Databricks cluster
# MAGIC 
# MAGIC In order to run this notebook properly, it must be attached to an Azure Databricks cluster that satisfies the following requirements:
# MAGIC   
# MAGIC - Python 3 must be installed

# COMMAND ----------

# MAGIC %md ### Install MLflow
# MAGIC 
# MAGIC Next, install the MLflow Python library using the following steps:
# MAGIC 
# MAGIC 1. Create the library with the Source `Upload Python Egg or PyPI` and the versioned Pip library name:
# MAGIC   - `mlflow>=0.8.0`
# MAGIC   
# MAGIC 2. Attach the library to the cluster.

# COMMAND ----------

# MAGIC %md ### Install the Azure ML SDK
# MAGIC 
# MAGIC Once a cluster has been launched with the configuration described in **Launch an Azure Databricks cluster**, install the Azure Machine Learning SDK using the following steps:
# MAGIC 
# MAGIC 1. Create the library with the Source ``Upload Python Egg or PyPI`` and the Pip library name:
# MAGIC   - `azureml-sdk[databricks]`     
# MAGIC      
# MAGIC 2. Attach the library to the cluster.

# COMMAND ----------

# MAGIC %md ### Create or load an Azure ML Workspace

# COMMAND ----------

# MAGIC %md Before models can be deployed to Azure ML, an Azure ML Workspace must be created or obtained. The `azureml.core.Workspace.create()` function will load a workspace of a specified name or create one if it does not already exist. For more information about creating an Azure ML Workspace, see the [Azure ML Workspace management documentation](https://docs.microsoft.com/en-us/azure/machine-learning/service/how-to-manage-workspace).

# COMMAND ----------

import azureml
from azureml.core import Workspace

# workspace_name = "<WORKSPACE_NAME>"
# workspace_location="<WORKSPACE_LOCATION>"
# resource_group = "<RESOURCE_GROUP>"
# subscription_id = "<SUBSCRIPTION_ID>"

workspace_name = "ravi-azureml"
workspace_location = "eastus2"
resource_group = "RaviS-rg"
subscription_id = "3f2e4d32-8e8d-46d6-82bc-5bb8d962328b"


workspace = Workspace.create(name = workspace_name,
                             location = workspace_location,
                             resource_group = resource_group,
                             subscription_id = subscription_id,
                             exist_ok=True)

# COMMAND ----------

# MAGIC %md ### (Optional) Connect to an MLflow tracking server
# MAGIC 
# MAGIC MLflow can collect data about a model training session, such as validation accuracy. It can also save artifacts produced during the training session, such as a PySpark pipeline model.
# MAGIC 
# MAGIC By default, these data and artifacts are stored on the cluster's local filesystem. However, they can also be stored remotely using an [MLflow Tracking Server](https://mlflow.org/docs/latest/tracking.html).

# COMMAND ----------

import mlflow

# Using the hosted mlflow tracking server

# COMMAND ----------

# MAGIC %md ## Training a model

# COMMAND ----------

# MAGIC %md ### Download training data 
# MAGIC 
# MAGIC First, download the [wine qualities dataset (published by Cortez et al.)](https://archive.ics.uci.edu/ml/datasets/wine+quality) that will be used to train the model.

# COMMAND ----------

# MAGIC %sh wget https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv

# COMMAND ----------

# MAGIC %md ### In an MLflow run, train and save an ElasticNet model for rating wines
# MAGIC 
# MAGIC We will train a model using Scikit-learn's Elastic Net regression module. We will fit the model inside a new MLflow run (training session), allowing us to save performance metrics, hyperparameter data, and model artifacts for future reference. If MLflow has been connected to a tracking server, this data will be persisted to the tracking server's file and artifact stores, allowing other users to view and download it. For more information about model tracking in MLflow, see the [MLflow tracking reference](https://www.mlflow.org/docs/latest/tracking.html).
# MAGIC 
# MAGIC Later, we will use the saved MLflow model artifacts to deploy the trained model to Azure ML for real-time serving.

# COMMAND ----------

import os
import warnings
import sys

import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet

import mlflow
import mlflow.sklearn


def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


def train_model(wine_data_path, model_path, alpha, l1_ratio):
    warnings.filterwarnings("ignore")
    np.random.seed(40)

    # Read the wine-quality csv file (make sure you're running this from the root of MLflow!)
    data = pd.read_csv(wine_data_path, sep=None)

    # Split the data into training and test sets. (0.75, 0.25) split.
    train, test = train_test_split(data)

    # The predicted column is "quality" which is a scalar from [3, 9]
    train_x = train.drop(["quality"], axis=1)
    test_x = test.drop(["quality"], axis=1)
    train_y = train[["quality"]]
    test_y = test[["quality"]]

    # Start a new MLflow training run 
    with mlflow.start_run():
        # Fit the Scikit-learn ElasticNet model
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        lr.fit(train_x, train_y)

        predicted_qualities = lr.predict(test_x)

        # Evaluate the performance of the model using several accuracy metrics
        (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

        print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
        print("  RMSE: %s" % rmse)
        print("  MAE: %s" % mae)
        print("  R2: %s" % r2)

        # Log model hyperparameters and performance metrics to the MLflow tracking server
        # (or to disk if no)
        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)

        mlflow.sklearn.log_model(lr, model_path)
        
        return mlflow.active_run().info.run_uuid

# COMMAND ----------

alpha_1 = 0.75
l1_ratio_1 = 0.25
model_path = "model"
run_id1 = train_model(wine_data_path=wine_data_path, model_path=model_path, alpha=alpha_1, l1_ratio=l1_ratio_1)

# COMMAND ----------

# MAGIC %md ## Building an Azure Container Image for model deployment

# COMMAND ----------

# MAGIC %md ### Use MLflow to build a Container Image for the trained model
# MAGIC 
# MAGIC We will use the `mlflow.azuereml.build_image` function to build an Azure Container Image for the trained MLflow model. This function also registers the MLflow model with a specified Azure ML workspace. The resulting image can be deployed to Azure Container Instances (ACI) or Azure Kubernetes Service (AKS) for real-time serving.

# COMMAND ----------

import mlflow.azureml

model_image, azure_model = mlflow.azureml.build_image(model_path=model_path, 
                                                      workspace=workspace, 
                                                      run_id=run_id1,
                                                      model_name="wine-model",
                                                      image_name="wine-model",
                                                      description="Sklearn ElasticNet image for rating wines", 
                                                      tags={
                                                        "alpha": str(alpha_1),
                                                        "l1_ratio": str(l1_ratio_1),
                                                      },
                                                      synchronous=False)

# COMMAND ----------

model_image.wait_for_creation(show_output=True)

# COMMAND ----------

# MAGIC %md ## Deploying the model to "DEV" using [Azure Container Instances (ACI)](https://docs.microsoft.com/en-us/azure/container-instances/)
# MAGIC 
# MAGIC The [ACI platform](https://docs.microsoft.com/en-us/azure/container-instances/) is the recommended environment for staging and developmental model deployments.

# COMMAND ----------

# MAGIC %md ### Create an ACI webservice deployment using the model's Container Image
# MAGIC 
# MAGIC Using the Azure ML SDK, we will deploy the Container Image that we built for the trained MLflow model to ACI.

# COMMAND ----------

from azureml.core.webservice import AciWebservice, Webservice

dev_webservice_name = "wine-model-dev"
dev_webservice_deployment_config = AciWebservice.deploy_configuration()
dev_webservice = Webservice.deploy_from_image(name=dev_webservice_name, image=model_image, deployment_config=dev_webservice_deployment_config, workspace=workspace)

# COMMAND ----------

dev_webservice.wait_for_deployment()

# COMMAND ----------

# MAGIC %md ## Querying the deployed model in "dev"

# COMMAND ----------

# MAGIC %md ### Load a sample input vector from the wine dataset

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn import datasets

data = pd.read_csv(wine_data_path, sep=None)
train, _ = train_test_split(data)
train_x = train.drop(["quality"], axis=1)
sample = train_x.iloc[[0]]
query_input = list(sample.as_matrix().flatten())
sample_json = sample.to_json(orient="split")

# COMMAND ----------

# MAGIC %md #### Evaluate the sample input vector by sending an HTTP request
# MAGIC We will query the ACI webservice's scoring endpoint by sending an HTTP POST request that contains the input vector.

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
  response = requests.post(scoring_uri, data=inputs, headers=headers)
  print("Response: {}".format(response.text))
  preds = json.loads(response.text)
  print("Received response: {}".format(preds))
  return preds

# COMMAND ----------

dev_scoring_uri = dev_webservice.scoring_uri

# COMMAND ----------

dev_prediction = query_endpoint_example(scoring_uri=dev_scoring_uri, inputs=sample_json)

# COMMAND ----------

# MAGIC %md ## Deploying the model to production using [Azure Kubernetes Service (AKS)](https://azure.microsoft.com/en-us/services/kubernetes-service/)

# COMMAND ----------

# MAGIC %md ### Option 1: Create a new AKS cluster
# MAGIC 
# MAGIC If you do not have an active AKS cluster for model deployment, you can create one using the Azure ML SDK.

# COMMAND ----------

from azureml.core.compute import AksCompute, ComputeTarget

# Use the default configuration (you can also provide parameters to customize this)
prov_config = AksCompute.provisioning_configuration()

aks_cluster_name = "wine-prod" 
# Create the cluster
aks_target = ComputeTarget.create(workspace = workspace, 
                                  name = aks_cluster_name, 
                                  provisioning_configuration = prov_config)

# Wait for the create process to complete
aks_target.wait_for_completion(show_output = True)
print(aks_target.provisioning_state)
print(aks_target.provisioning_errors)

# COMMAND ----------

# MAGIC %md ### Option 2: Connect to an existing AKS cluster
# MAGIC 
# MAGIC If you already have any active AKS cluster running, you can add it to your Workspace using the Azure ML SDK.

# COMMAND ----------

from azureml.core.compute import AksCompute, ComputeTarget

# Get the resource id from https://porta..azure.com -> Find your resource group -> click on the Kubernetes service -> Properties
#resource_id = "/subscriptions/<your subscription id>/resourcegroups/<your resource group>/providers/Microsoft.ContainerService/managedClusters/<your aks service name>"
resource_id = "/subscriptions/3f2e4d32-8e8d-46d6-82bc-5bb8d962328b/resourcegroups/jahubba-azuresdk-east/providers/Microsoft.ContainerService/managedClusters/jahubba-k8s"

# Give the cluster a local name
#cluster_name = "<CLUSTER_NAME>"
cluster_name = "mldeploy3"

# Attatch the cluster to your workgroup
#aks_target = AksCompute.attach(workspace=workspace, name=cluster_name, resource_id=resource_id)
attach_config = AksCompute.attach_configuration(resource_group="jahubba-azuresdk-east",
                                                cluster_name="jahubba-k8s")
compute = ComputeTarget.attach(workspace, cluster_name, attach_config)

# Wait for the operation to complete
compute.wait_for_completion(True)
print(compute.provisioning_state)
print(compute.provisioning_errors)

# COMMAND ----------

# MAGIC %md ### Deploy to the model's image to the specified AKS cluster

# COMMAND ----------

from azureml.core.webservice import Webservice, AksWebservice

# Set configuration and service name
prod_webservice_name = "wine-model-prod"
prod_webservice_deployment_config = AksWebservice.deploy_configuration()

# Deploy from image
prod_webservice = Webservice.deploy_from_image(workspace = workspace, 
                                               name = prod_webservice_name,
                                               image = model_image,
                                               deployment_config = prod_webservice_deployment_config,
                                               deployment_target = aks_target)

# COMMAND ----------

# Wait for the deployment to complete
prod_webservice.wait_for_deployment(show_output = True)

# COMMAND ----------

# MAGIC %md ## Querying the deployed model in production

# COMMAND ----------

# MAGIC %md ### Load a sample input vector from the wine dataset

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn import datasets

data = pd.read_csv(wine_data_path, sep=None)
train, _ = train_test_split(data)
train_x = train.drop(["quality"], axis=1)
sample = train_x.iloc[[0]]
query_input = list(sample.as_matrix().flatten())
sample_json = sample.to_json(orient="split")

# COMMAND ----------

# MAGIC %md #### Evaluate the sample input vector by sending an HTTP request
# MAGIC We will query the AKS webservice's scoring endpoint by sending an HTTP POST request that includes the input vector. The production AKS deployment may require an authorization token (service key) for queries. We will include this key in the HTTP request header.

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
  response = requests.post(scoring_uri, data=inputs, headers=headers)
  preds = json.loads(response.text)
  print("Received response: {}".format(preds))
  return preds

# COMMAND ----------

prod_scoring_uri = prod_webservice.scoring_uri
prod_service_key = prod_webservice.get_keys()[0] if len(prod_webservice.get_keys()) > 0 else None

# COMMAND ----------

prod_prediction = query_endpoint_example(scoring_uri=prod_scoring_uri, service_key=prod_service_key, inputs=sample_json)

# COMMAND ----------

# MAGIC %md ## Updating the production deployment

# COMMAND ----------

# MAGIC %md ### Train a new model
# MAGIC First, we will train a new ElasticNet model with updated hyperparameters.

# COMMAND ----------

alpha_2 = 0.5
l1_ratio_2 = 0.8
model_path = "model"
run_id1 = train_model(wine_data_path=wine_data_path, model_path=model_path, alpha=alpha_2, l1_ratio=l1_ratio_2)

# COMMAND ----------

# MAGIC %md ### Build an Azure Container Image for the new model

# COMMAND ----------

import mlflow.azureml

model_image_updated, azure_model_updated = mlflow.azureml.build_image(model_path=model_path, 
                                                                      workspace=workspace, 
                                                                      run_id=run_id1,
                                                                      model_name="wine-model",
                                                                      image_name="wine-model",
                                                                      description="Sklearn ElasticNet image for rating wines", 
                                                                      tags={
                                                                        "alpha": str(alpha_2),
                                                                        "l1_ratio": str(l1_ratio_2),
                                                                      },
                                                                      synchronous=False)

# COMMAND ----------

model_image_updated.wait_for_creation(show_output=True)

# COMMAND ----------

# MAGIC %md ### Deploy the new model's image to the AKS cluster
# MAGIC 
# MAGIC Using the [azureml.core.webservice.AksWebservice.update()](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.webservice.akswebservice?view=azure-ml-py#update) function, we will replace the deployment's existing model image with the new model image.

# COMMAND ----------

prod_webservice.update(image=model_image_updated)

# COMMAND ----------

prod_webservice.wait_for_deployment(show_output = True)

# COMMAND ----------

# MAGIC %md ### Query the updated model

# COMMAND ----------

prod_prediction_updated = query_endpoint_example(scoring_uri=prod_scoring_uri, service_key=prod_service_key, inputs=sample_json)

# COMMAND ----------

# MAGIC %md ## Cleaning up the deployments

# COMMAND ----------

# MAGIC %md ### Terminate the "dev" ACI webservice
# MAGIC 
# MAGIC Because ACI manages compute resources on your behalf, deleting the "dev" ACI webservice will remove all resources associated with the "dev" model deployment

# COMMAND ----------

dev_webservice.delete()

# COMMAND ----------

# MAGIC %md ### Terminate the production AKS webservice
# MAGIC 
# MAGIC This terminates the real-time serving webservice running on the specified AKS cluster. It **does not** terminate the AKS cluster.

# COMMAND ----------

prod_webservice.delete()

# COMMAND ----------

# MAGIC %md ### Remove the AKS cluster from the Azure ML Workspace
# MAGIC 
# MAGIC If the cluster was created using the Azure ML SDK (see **Option 1: Create a new AKS cluster**), removing it from the Azure ML Workspace will terminate the cluster, including all of its compute resources and deployments.
# MAGIC 
# MAGIC If the cluster was created independently (see **Option 2: Connect to an existing AKS cluster**), it will remain active after removal from the Azure ML Workspace.

# COMMAND ----------

aks_target.delete()

# COMMAND ----------

