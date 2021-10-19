# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # MLflow
# MAGIC 
# MAGIC As you might have noticed, throughout the day you tried different model architectures. But how do you remember which one worked best? That's where [MLflow](https://mlflow.org/) comes into play!
# MAGIC 
# MAGIC [MLflow](https://mlflow.org/docs/latest/concepts.html) seeks to address these three core issues:
# MAGIC 
# MAGIC * It’s difficult to keep track of experiments
# MAGIC * It’s difficult to reproduce code
# MAGIC * There’s no standard way to package and deploy models
# MAGIC 
# MAGIC In this notebook, we will show how to do experiment tracking with MLflow! We will start with logging the metrics from the models we created with the California housing dataset today.
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Log experiments with MLflow
# MAGIC  - View MLflow UI
# MAGIC  - Generate a UDF with MLflow

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install MLflow on Your Databricks Cluster
# MAGIC 
# MAGIC 1. Add `mlflow` as a PyPi library in Databricks, and install it on your cluster
# MAGIC   * Follow [Upload a Python PyPI package or Python Egg](https://docs.azuredatabricks.net/user-guide/libraries.html#upload-a-python-pypi-package-or-python-egg) to create a library
# MAGIC   * Choose **PyPi** and enter `mlflow==0.8.2` (this notebook was tested with `mlflow` version 0.8.2)

# COMMAND ----------

# MAGIC %run "./Includes/Classroom Setup"

# COMMAND ----------

# Wait for the mlflow module to attactch to our cluster
# Utility method defined in Classroom-Setup
waitForMLflow()

# COMMAND ----------

from sklearn.datasets.california_housing import fetch_california_housing
from sklearn.model_selection import train_test_split
import numpy as np
np.random.seed(0)
import tensorflow as tf
tf.set_random_seed(42) # For reproducibility

cal_housing = fetch_california_housing()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(cal_housing.data,
                                                        cal_housing.target,
                                                        test_size=0.2,
                                                        random_state=1)

print(cal_housing.DESCR)

# COMMAND ----------

# MAGIC %md
# MAGIC Build model architecture as before.

# COMMAND ----------

from keras.models import Sequential
from keras.layers import Dense

def build_model():
  return Sequential([Dense(20, input_dim=8, activation='relu'),
                    Dense(20, activation='relu'),
                    Dense(1, activation='linear')]) # Keep the last layer as linear because this is a regression problem

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Using MLflow in a Notebook

# COMMAND ----------

import mlflow
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

experimentPath = "/Users/" + username + "/experiment-mlflow"

try:
  experimentID = mlflow.create_experiment(experimentPath)
  
except MlflowException: # if experiment is already created
  experimentID = MlflowClient().get_experiment_by_name(experimentPath).experiment_id
  mlflow.set_experiment(experimentPath)

print(f"The experiment can be found at the path {experimentPath} and has an experiment_id of {experimentID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Track experiments!

# COMMAND ----------

# Note issue with **kwargs https://github.com/keras-team/keras/issues/9805
from mlflow.keras import log_model

def trackExperiments(experimentID, run_name, model, compile_kwargs, fit_kwargs, optional_params={}):
  '''
  This is a wrapper function for tracking expirements with MLflow
    
  Parameters
  ----------
  model: Keras model
    The model to track
    
  compile_kwargs: dict
    Keyword arguments to compile model with
  
  fit_kwargs: dict
    Keyword arguments to fit model with
  '''
  with mlflow.start_run(experiment_id=experimentID, run_name=run_name) as run:
    model = model()
    model.compile(**compile_kwargs)
    history = model.fit(**fit_kwargs)
    
    for param_key, param_value in {**compile_kwargs, **fit_kwargs, **optional_params}.items():
      if param_key not in ["x", "y", "X_val", "y_val"]:
        mlflow.log_param(param_key, param_value)
    
    for key, values in history.history.items():
      for v in values:
          if not np.isnan(v): # MLflow won't log NaN
            mlflow.log_metric(key, v)

    for i, layer in enumerate(model.layers):
      mlflow.log_param("hidden_layer_" + str(i) + "_units", layer.output_shape)
      
    log_model(model, "/tmp/keras_model")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's recall what happened when we used SGD.

# COMMAND ----------

compile_kwargs = {
  "optimizer": "sgd", 
  "loss": "mse",
  "metrics": ["mse", "mae"],
}

fit_kwargs = {
  "x": X_train, 
  "y": y_train,
  "epochs": 10,
  "verbose": 2
}

run_name = "SGD"
trackExperiments(experimentID, run_name, build_model, compile_kwargs, fit_kwargs)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's change the optimizer

# COMMAND ----------

compile_kwargs["optimizer"] = "adam" 

run_name = "ADAM"
trackExperiments(experimentID, run_name, build_model, compile_kwargs, fit_kwargs)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's add some data normalization, as well as a validation dataset.

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)

fit_kwargs["validation_split"] = 0.2
fit_kwargs["x"] = X_train_scaled

optional_params = {
  "normalize_data": "true"
}

run_name = "NormalizedValidation"
trackExperiments(experimentID, run_name, build_model, compile_kwargs, fit_kwargs, optional_params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review the MLflow UI

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Past Runs
# MAGIC 
# MAGIC You can query past runs programatically in order to use this data back in Python.  The pathway to doing this is an `MlflowClient` object. 

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()

# COMMAND ----------

client.list_experiments()

# COMMAND ----------

# MAGIC %md
# MAGIC Now list all the runs for your experiment using `.list_run_infos()`, which takes your experiment id as a parameter.

# COMMAND ----------

import pandas as pd

runs = pd.DataFrame([(run.run_uuid, run.start_time, run.artifact_uri) for run in client.list_run_infos(experimentID)])
runs.columns = ["run_uuid", "start_time", "artifact_uri"]

display(runs)

# COMMAND ----------

# MAGIC %md
# MAGIC Pull the last run and take a look at the associated artifacts.

# COMMAND ----------

last_run = runs.sort_values("start_time", ascending=False).iloc[0]
dbutils.fs.ls(last_run["artifact_uri"] + "/tmp/keras_model")

# COMMAND ----------

# MAGIC %md
# MAGIC Return the evaluation metrics for the last run.

# COMMAND ----------

client.get_run(last_run.run_uuid).data.metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Defined Function
# MAGIC 
# MAGIC Let's now register our Keras model as a Spark UDF to apply to rows in parallel.

# COMMAND ----------

import pandas as pd

path = client.get_run(last_run.run_uuid).info.artifact_uri.replace("dbfs:/", "/dbfs/") + "/tmp/keras_model"
predict = mlflow.pyfunc.spark_udf(spark, path = path)

X_test_DF = spark.createDataFrame(pd.DataFrame(X_test))

display(X_test_DF.withColumn("prediction", predict("0", "1", "2", "3", "4", "5", "6", "7")))

# COMMAND ----------

spark.udf.register("predictUDF", predict)
X_test_DF.createOrReplaceGlobalTempView("X_test_DF")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, predictUDF(*) as prediction from global_temp.X_test_DF

# COMMAND ----------

# MAGIC %md
# MAGIC Now, go back and add MLflow to your experiments from the Boston Housing Dataset!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>