# Databricks notebook source
def set_experiment(experiment_name):
    print("experiment_name:",experiment_name)
    if experiment_name == "":
        experiment_id = None
    else:
        mlflow.set_experiment(experiment_name)
        experiment_id = mlflow_client.get_experiment_by_name(experiment_name).experiment_id
    print("experiment_id:",experiment_id)
    return experiment_id

# COMMAND ----------

import mlflow
print("MLflow Version:", mlflow.__version__)
mlflow_client = mlflow.tracking.MlflowClient()

# COMMAND ----------

def get_notebook_tag(tag):
    tag = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get(tag)
    return None if tag.isEmpty() else tag.get()

# COMMAND ----------

# If running from within notebook.run(), host_name will be unknown
def create_run_uri(experiment_id, run_id):
    host_name = get_notebook_tag("browserHostName")
    host_name = host_name if host_name else "_?_"
    return f"https://{host_name}/#mlflow/experiments/{experiment_id}/runs/{run_id}"

# COMMAND ----------

# Utility function to display run URI
# Example: https://demo.cloud.databricks.com/#mlflow/experiments/2580010/runs/5cacf3cbad89413d8167cfe54eaec8dd

def display_run_uri(experiment_id, run_id):
    uri = create_run_uri(experiment_id, run_id)
    displayHTML(f'Run URI: <a href="{uri}">{uri}</a>')

# COMMAND ----------

def get_username():
    return get_notebook_tag("user")
  
def get_workspace_home_dir():
    return f"/Users/{get_username()}"

# COMMAND ----------

EXPERIMENT_NAME_PREFIX = "onnx"
def create_experiment_name(name, prefix=EXPERIMENT_NAME_PREFIX):
    return f"{get_workspace_home_dir()}/{prefix}_{name}"

# COMMAND ----------

import os, platform
import mlflow
import pyspark
from mlflow.entities import RunTag
  
def create_version_tags():
    return [RunTag("mlflow_version", mlflow.__version__), 
           RunTag("spark_version", spark.version),
           RunTag("sparkVersion", str(get_notebook_tag("sparkVersion"))),
           RunTag("dbr_version", os.environ.get('DATABRICKS_RUNTIME_VERSION',None)),
           RunTag("pyspark_version", pyspark.__version__),
           RunTag("python_version", platform.python_version())
           ]

# COMMAND ----------

import os
import requests

# Example:
#  data_path = "/dbfs/tmp/john.doe@acme.com/tmp/mlflow_demo/wine-quality.csv"
#  data_url = "https://raw.githubusercontent.com/mlflow/mlflow/master/examples/sklearn_elasticnet_wine/wine-quality.csv"

def download_file(data_uri, data_path):
    if os.path.exists(data_path):
        print("File {} already exists".format(data_path))
    else:
        print(f"Downloading {data_uri} to {data_path}")
        rsp = requests.get(data_uri)
        with open(data_path, 'w') as f:
            f.write(rsp.text)

# COMMAND ----------

user = get_notebook_tag("user")

if user == "unknown": # if not running as notebook, i.e. %run or as job
    base_dir_fuse = "/dbfs/tmp/mlflow_demo"
else:
    base_dir_fuse = f"/dbfs/tmp/{user}/mlflow_demo"

    os.makedirs(base_dir_fuse, exist_ok=True)

# COMMAND ----------

def download_wine_file():
    data_path = f"{base_dir_fuse}/wine-quality.csv"
    data_uri = "https://raw.githubusercontent.com/mlflow/mlflow/master/examples/sklearn_elasticnet_wine/wine-quality.csv"
    download_file(data_uri, data_path)
    return data_path

# COMMAND ----------

def read_wine_data():
    data_path = download_wine_file()
    return spark.read.format("csv") \
      .option("header", "true") \
      .option("inferSchema", "true") \
      .load(data_path.replace("/dbfs","dbfs:")) 

# COMMAND ----------

import matplotlib.pyplot as plt

def create_plot_file(y_test_set, y_predicted, plot_file):
    global image
    fig, ax = plt.subplots()
    ax.scatter(y_test_set, y_predicted, edgecolors=(0, 0, 0))
    ax.plot([y_test_set.min(), y_test_set.max()], [y_test_set.min(), y_test_set.max()], 'k--', lw=4)
    ax.set_xlabel('Actual')
    ax.set_ylabel('Predicted')
    ax.set_title("Ground Truth vs Predicted")
    image = fig
    fig.savefig(plot_file)
    plt.close(fig)