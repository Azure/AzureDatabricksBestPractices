# Databricks notebook source
import mlflow
import mlflow.azureml
import azureml.mlflow
import azureml.core
from azureml.core import Workspace
from azureml.mlflow import get_portal_url

print("AzureML SDK version:", azureml.core.VERSION)
print("MLflow version:", mlflow.version.VERSION)

# COMMAND ----------

from azureml.core.authentication import ServicePrincipalAuthentication
def service_principal_auth():
  return ServicePrincipalAuthentication(
      tenant_id=tenant_id,
      service_principal_id=sp_id,
      service_principal_password=sp_secret)
  

# COMMAND ----------

from azureml.core.authentication import InteractiveLoginAuthentication
def interactive_auth():
  return InteractiveLoginAuthentication(tenant_id=tenant_id)

# COMMAND ----------

def azureml_workspace(auth_type='interactive'):
  if auth_type == 'interactive':
    auth = interactive_auth()
  elif auth_type == 'service_princpal':
    auth = service_principal_auth()
    
  ws = Workspace.create(name = workspace_name,
                       resource_group = resource_group,
                       subscription_id = subscription_id,
                       exist_ok=True,
                       auth=auth)
  return ws

# COMMAND ----------

def auzreml_mlflow_tracking_uri(workspace):
  return workspace.get_mlflow_tracking_uri()

# COMMAND ----------

from azureml.core.webservice import AciWebservice, Webservice
import random
import string

def azureml_build_deploy(runid, workspace, model_name, image_name, deploy_name):
  # Build an Azure ML Container Image for an MLflow 
  azure_image, azure_model = mlflow.azureml.build_image(model_uri='runs:/{}/{}'.format(runid, MODEL_SAVE_PATH),
                                                      workspace=workspace,
                                                      model_name=model_name,
                                                      image_name=image_name,
                                                      synchronous=True)
  
  
  # Deploy the image to Azure Container Instances (ACI) for real-time serving
  aci_config = AciWebservice.deploy_configuration()
  deployment_stub = ''.join([random.choice(string.ascii_lowercase) for i in range(5)])
  print("Deploying as "+deploy_name+"-"+deployment_stub)
  webservice = Webservice.deploy_from_image(
      image=azure_image, workspace=workspace, name=deploy_name+"-"+deployment_stub, deployment_config=aci_config)

  webservice.wait_for_deployment()
  
  return webservice