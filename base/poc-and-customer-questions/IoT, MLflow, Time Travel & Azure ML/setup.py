# Databricks notebook source
#dbutils.fs.put("dbfs:/databricks/init/aweaver/azureml.sh" ,"""
#!/bin/bash
#rm -rf  /databricks/python3/lib/python3.5/site-packages/OpenSSL
#rm -rf  /databricks/python3/lib/python3.5/site-packages/pyOpenSSL-*.egg-info
#/databricks/python3/bin/pip install --upgrade pip
#/databricks/python3/bin/pip uninstall cryptograhy -y
#/databricks/python3/bin/pip install cryptograhy
#/databricks/python3/bin/pip install pyOpenSSL""", True)

# COMMAND ----------

import mlflow
import os

def init_mlflow():
  databricks_host = 'https://eastus2.azuredatabricks.net'
  databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  os.environ['DATABRICKS_HOST'] = databricks_host
  os.environ['DATABRICKS_TOKEN'] = databricks_token
  mlflow.set_tracking_uri("databricks")
  print("Using MLflow version ", mlflow.version.VERSION)
  mlflow.set_experiment("Wind Turbines")
  return mlflow

# COMMAND ----------

from azureml.core.authentication import ServicePrincipalAuthentication

def get_service_principal():
  sp = ServicePrincipalAuthentication("9f37a392-f0ae-4280-9796-f1864a10effc", "29844ed9-47a1-490d-ac2c-6f000b18ba33", "4jLp3BIl4Q5KtfBIWNch0zyoXqIKmDscjnlTLpT/Z48=")
  print(sp.get_authentication_header())
  return sp

# COMMAND ----------

from azureml.core import Workspace

def get_azure_workspace(sp): 
  azure_workspace = Workspace.get(name="IoT_Models", auth=sp, subscription_id="3f2e4d32-8e8d-46d6-82bc-5bb8d962328b", resource_group="field-eng")
  return azure_workspace