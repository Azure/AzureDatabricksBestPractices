# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Running a Project within a Project

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experiment: `experiment-L3`
# MAGIC 
# MAGIC Run the following cell to re-open/create the `experiment-L3` experiment which contains the logged runs from notebook` 03-Packaging-ML-Projects`. Have the UI for this experiment open in a different tab.

# COMMAND ----------

import mlflow
from mlflow.exceptions import MlflowException
from  mlflow.tracking import MlflowClient

experimentPath = "/Users/" + username + "/experiment-L3"

try:
  experimentID = mlflow.create_experiment(experimentPath)
except MlflowException:
  experimentID = MlflowClient().get_experiment_by_name(experimentPath).experiment_id
  mlflow.set_experiment(experimentPath)

print("The experimment can be found at the path `{}` and has an experiment_id of `{}`".format(experimentPath, experimentID))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for Appropriate Files
# MAGIC 
# MAGIC Run the following 2 cells to check that your `tmp` folder still contains the files created and saved in the 03 notebook.
# MAGIC 
# MAGIC Under `train_path`, you should have the following 3 files: 
# MAGIC * `MLproject`
# MAGIC * `conda.yaml`
# MAGIC * `train.py`
# MAGIC 
# MAGIC Under `load_path`, you should have the following 3 files: 
# MAGIC * `MLproject`
# MAGIC * `conda.yaml`
# MAGIC * `load.py`

# COMMAND ----------

train_path = "/tmp/" + username + "/mlflow-model-training/"
dbutils.fs.ls(train_path)

# COMMAND ----------

load_path = "/tmp/" + username + "/mlflow-data-loading/"
dbutils.fs.ls(load_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call Project at End of first Project Call
# MAGIC 
# MAGIC At the end of notebook 3, we retrieved the data path artifact saved from the logged data loading run and used that as the `data_path` parameter of our training code. We did this by making a separate `mlflow.projects.run` call on the driver node of our Spark cluster.
# MAGIC 
# MAGIC Now we want log the same projects but through only 1 explicit `mlflow.projects.run` call by having the first project call the 2nd project. We would also like to run these projects code on a new Spark cluster (specified by the given `clusterspecs` below).
# MAGIC 
# MAGIC First edit the following `data_load` function to take in `train_path` as an additional parameter and call the MLproject saved at `train_path` as its last step.

# COMMAND ----------

# TODO

def data_load(data_input_path, FILL_IN):

  with mlflow.start_run() as run:
    # Log the data
    mlflow.log_artifact(data_input_path, "data-csv-dir")
  # FILL IN

if __name__ == "__main__":
  data_load( "/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv", train_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Double check that the UI correctly logged your 2 project runs (the data loading and then training) from above by comparing it to the last 2 runs from the end of the 03 notebook.
# MAGIC 
# MAGIC Then fill in the below code to overwrite the original `load.py` file to have the new `data_load(data_input_path, train_path)` function. Be sure to add an appropriate `@click.option` for the new `train_path` parameter.

# COMMAND ----------

#  TODO
dbutils.fs.put(load_path + "/load.py", 
'''
import click
import mlflow

@click.command()
@click.option("--data_input_path", default="/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv", type=str)
@click.option(FILL_IN)
def data_load(data_input_path, FILL_IN):
  # REPLACE WITH data_load FUNCTION WRITTEN ABOVE

if __name__ == "__main__":
  data_load()

'''.strip(), overwrite = True)

dbutils.fs.ls(load_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Lastly, fill in the following single `mlflow.projects.run` call to directly run the loading data project which should then indirectly invoke the training code, all on a new Spark cluster (specified by the given `clusterspecs` below). 

# COMMAND ----------

# TODO

clusterspecs = {
    "num_workers": 2,
    "spark_version": "5.2.x-scala2.11",
    "spark_conf": {},
    "aws_attributes": {
        "first_on_demand": 1,
        "availability": "SPOT_WITH_FALLBACK",
        "zone_id": "us-west-1c",
        "spot_bid_price_percent": 100,
        "ebs_volume_count": 0
    },
    "node_type_id": "i3.xlarge",
    "driver_node_type_id": "i3.xlarge"
  }

mlflow.projects.run(FILL_IN)

# COMMAND ----------

# MAGIC %md
# MAGIC Check that these logged run also show up properly on the MLflow UI. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>