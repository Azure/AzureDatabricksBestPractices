# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # MLflow Lab
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Add MLflow to your experiments from the Boston Housing Dataset!
# MAGIC  
# MAGIC **Bonus:**
# MAGIC * Create LambdaCallback to log MLflow metrics while the model is training (after each epoch)
# MAGIC * Create a UDF that you can invoke in SQL
# MAGIC * Get the lowest MSE!

# COMMAND ----------

# MAGIC %run "./Includes/Classroom Setup"

# COMMAND ----------

# Wait for the mlflow module to attactch to our cluster
# Utility method defined in Classroom-Setup
waitForMLflow()

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.datasets import load_boston
import numpy as np
np.random.seed(0)

boston_housing = load_boston()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(boston_housing.data,
                                                        boston_housing.target,
                                                        test_size=0.2,
                                                        random_state=1)

print(boston_housing.DESCR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build_model
# MAGIC Create a `build_model()` function. Because Keras models are stateful, we want to get a fresh model every time we are trying out a new experiment.

# COMMAND ----------

import tensorflow as tf
tf.set_random_seed(42) # For reproducibility

from keras.models import Sequential
from keras.layers import Dense

def build_model():
  return Sequential([Dense(50, input_dim=13, activation='relu'),
                    Dense(20, activation='relu'),
                    Dense(1, activation='linear')]) # Keep the last layer as linear because this is a regression problem

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Using MLflow in a Notebook

# COMMAND ----------

import mlflow
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

experimentPath = "/Users/" + username + "/experiment-mlflow-lab"

try:
  experimentID = mlflow.create_experiment(experimentPath)
  
except MlflowException: # if experiment is already created
  experimentID = MlflowClient().get_experiment_by_name(experimentPath).experiment_id
  mlflow.set_experiment(experimentPath)



print(f"The experimment can be found at the path {experimentPath} and has an experiment_id of {experimentID}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Track experiments!

# COMMAND ----------

# ANSWER
from mlflow.keras import log_model

run_name = ""
def trackExperiments(experimentID=experimentID, run_name=run_name, build_model=build_model, optimizer="adam", loss="mse", metrics=["mse"], epochs=10, batch_size=32, 
                     validation_split=0.0, validation_data=None, verbose=2, normalize_data=False, callbacks=None):
  with mlflow.start_run(experiment_id=experimentID, run_name=run_name) as run:
    model = build_model()
    model.compile(optimizer=optimizer, loss=loss, metrics=metrics)
    history = model.fit(X_train, y_train, epochs=epochs, verbose=verbose, validation_split=validation_split, validation_data=validation_data, callbacks=callbacks)

    mlflow.log_param("loss", loss)
    mlflow.log_param("optimizer", optimizer)
    mlflow.log_param("epochs", epochs)
    mlflow.log_param("batch_size", batch_size)
    mlflow.log_param("validation_split", validation_split)
    
    if normalize_data:
      mlflow.log_param("normalize_data", "true")
    
    for key, values in history.history.items():
      for v in values:
          mlflow.log_metric(key, v)

    for i, layer in enumerate(model.layers):
      mlflow.log_param("hidden_layer_" + str(i) + "_units", layer.output_shape)
      
    log_model(model, "/tmp/keras_model")

# COMMAND ----------

# ANSWER
trackExperiments(experimentID, "adam", optimizer="adam", loss='mse', metrics=["mse"], epochs=100, batch_size = 32)

# COMMAND ----------

# ANSWER
from sklearn.preprocessing import StandardScaler
from keras.callbacks import ModelCheckpoint, EarlyStopping

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

X_train, X_val, y_train, y_val = train_test_split(X_train,
                                                  y_train,
                                                  test_size=0.25,
                                                  random_state=1)

filepath = '/dbfs/user/' + username + '/keras_weights.hdf5'
dbutils.fs.rm(filepath, recurse=True)
checkpointer = ModelCheckpoint(filepath=filepath, verbose=1, save_best_only=True)
earlyStopping = EarlyStopping(monitor='val_loss', min_delta=0.0001, patience=2, mode='auto')

trackExperiments(metrics=["mae", "mse"], validation_data=(X_val, y_val), epochs=30, batch_size=32, verbose=2, callbacks=[checkpointer, earlyStopping], normalize_data=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Defined Function
# MAGIC 
# MAGIC Let's now register our Keras model as a Spark UDF to apply to rows in parallel.

# COMMAND ----------

# ANSWER
import pandas as pd
from mlflow.tracking import MlflowClient

client = MlflowClient()

runs = pd.DataFrame([(run.run_uuid, run.start_time, run.artifact_uri) for run in client.list_run_infos(experimentID)])
runs.columns = ["run_uuid", "start_time", "artifact_uri"]
last_run = runs.sort_values("start_time", ascending=False).iloc[0]

path = client.get_run(last_run.run_uuid).info.artifact_uri.replace("dbfs:/", "/dbfs/") + "/tmp/keras_model"
predict = mlflow.pyfunc.spark_udf(spark, path = path)

X_test_DF = spark.createDataFrame(pd.DataFrame(X_test))

display(X_test_DF.withColumn("prediction", predict("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")))


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>