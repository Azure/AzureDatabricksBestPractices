# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Management
# MAGIC 
# MAGIC An MLflow model is a standard format for packaging models that can be used on a variety of downstream tools.  This lesson provides a generalizable way of handling machine learning models created in and deployed to a variety of environments.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Introduce model management best practices
# MAGIC  - Store and use different flavors of models for different deployment environments
# MAGIC  - Apply models combined with arbitrary pre and post-processing code using Python models

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Managing Machine Learning Models
# MAGIC 
# MAGIC Once a model has been trained and bundled with the environment it was trained in, the next step is to package the model so that it can be used by a variety of serving tools.  The current deployment options include Docker-based REST servers, Spark using streaming or batch, and cloud platforms such as Azure ML and AWS SageMaker.  Packaging the final model in a platform-agnostic way offers the most flexibility in deployment options and allows for model reuse across a number of platforms.
# MAGIC 
# MAGIC **MLflow models is a convention for packaging machine learning models that offers self-contained code, environments, and models.**  The main abstraction in this package is the concept of **flavors,** which are different ways the model can be used.  For instance, a TensorFlow model can be loaded as a TensorFlow DAG or as a python function: using an MLflow model convention allows for both of theses flavors.
# MAGIC 
# MAGIC The primary difference between MLflow projects and models is that models are geared more towards inference and serving.  The `python_function` flavor of models gives a generic way of bundling models regardless of whether it was `sklearn`, `keras`, or any other machine learning library that trained the model.  We can thereby deploy a python function without worrying about the underlying format of the model.  **MLflow therefore maps any training framework to any deployment**, massively reducing the complexity of inference.
# MAGIC 
# MAGIC Finally, arbitrary pre and post-processing steps can be included in the pipeline such as data loading, cleansing, and featurization.  This means that the full pipeline, not just the model, can be preserved.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-models-enviornments.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Model Flavors
# MAGIC 
# MAGIC Flavors offer a way of saving models in a way that's agnostic to the training development, making it significantly easier to be used in various deployment options.  The current built-in flavors include the following:<br><br>
# MAGIC 
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#module-mlflow.pyfunc" target="_blank">mlflow.pyfunc</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.h2o.html#module-mlflow.h2o" target="_blank">mlflow.h2o</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.keras.html#module-mlflow.keras" target="_blank">mlflow.keras</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.pytorch.html#module-mlflow.pytorch" target="_blank">mlflow.pytorch</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.sklearn.html#module-mlflow.sklearn" target="_blank">mlflow.sklearn</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.spark.html#module-mlflow.spark" target="_blank">mlflow.spark</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.tensorflow.html#module-mlflow.tensorflow" target="_blank">mlflow.tensorflow</a>
# MAGIC 
# MAGIC Models also offer reproducibility since the run ID and the timestamp of the run are preserved as well.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-models.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC First create an experiement to work in.

# COMMAND ----------

import mlflow
from mlflow.exceptions import MlflowException
from  mlflow.tracking import MlflowClient

experimentPath = "/Users/" + username + "/experiment-L4"

try:
  experimentID = mlflow.create_experiment(experimentPath)
except MlflowException:
  experimentID = MlflowClient().get_experiment_by_name(experimentPath).experiment_id
  mlflow.set_experiment(experimentPath)

print("The experiment can be found at the path `{}` and has an experiment_id of `{}`".format(experimentPath, experimentID))

# COMMAND ----------

# MAGIC %md
# MAGIC Import the data

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv("/dbfs/mnt/conor-work/airbnb/airbnb-cleaned-mlflow.csv")
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

# COMMAND ----------

# MAGIC %md
# MAGIC Train a RF model.

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

rf = RandomForestRegressor(n_estimators=100, max_depth=5)
rf.fit(X_train, y_train)

rf_mse = mean_squared_error(y_test, rf.predict(X_test))

rf_mse

# COMMAND ----------

# MAGIC %md
# MAGIC Train a NN

# COMMAND ----------

import tensorflow as tf
tf.set_random_seed(42) # For reproducibility

from keras.models import Sequential
from keras.layers import Dense

nn = Sequential([
  Dense(40, input_dim=21, activation='relu'),
  Dense(40, activation='relu'),
  Dense(40, activation='relu'),
  Dense(1, activation='linear')
])

nn.compile(optimizer="adam", loss="mse")
nn.fit(X_train, y_train, validation_split=.2, epochs=40, verbose=2)

# COMMAND ----------

nn_mse = mean_squared_error(y_test, nn.predict(X_test))

nn_mse

# COMMAND ----------

# MAGIC %md
# MAGIC Now log the two models

# COMMAND ----------

# from databricks_cli.configure.provider import get_config
# import os

# os.environ["MLFLOW_AUTODETECT_EXPERIMENT_ID"] = 'true'
# os.environ['DATABRICKS_HOST'] = get_config().host
# os.environ['DATABRICKS_TOKEN'] = get_config().token

# COMMAND ----------

import mlflow.sklearn

# mlflow.set_experiment(experiment_name=experimentPath)

with mlflow.start_run(experiment_id=experimentID, run_name="RF Model") as run: 
  mlflow.sklearn.log_model(rf, "random-forest-model")
  mlflow.log_metric("mse", rf_mse)
  


# COMMAND ----------

import mlflow.keras

with mlflow.start_run(experiment_id=experimentID, run_name="NN Model") as run: 
  mlflow.keras.log_model(nn, "neural-network-model")
  mlflow.log_metric("mse", nn_mse)

# COMMAND ----------

# MAGIC %md
# MAGIC Grab the most recent 2 experiments

# COMMAND ----------

from  mlflow.tracking import MlflowClient

client = MlflowClient()
(nn_run, rf_run) = sorted(client.list_run_infos(experimentID), key=lambda r: r.start_time, reverse=True)[-2:]

# COMMAND ----------

# MAGIC %md
# MAGIC Look at the model flavors.  Both have their respective `keras` or `sklearn` flavors as well as a `python_funtion` flavor.

# COMMAND ----------

nn_path = nn_run.artifact_uri+"/neural-network-model/"
print(dbutils.fs.head(nn_path+"MLmodel"))

# COMMAND ----------

rf_path = rf_run.artifact_uri+"/random-forest-model/"
print(dbutils.fs.head(rf_path+"MLmodel"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can use both of these models in the same way, even though they were trained by different packages.

# COMMAND ----------

import mlflow.pyfunc

rf_pyfunc_model = mlflow.pyfunc.load_pyfunc(path=rf_path.replace("dbfs:", "/dbfs"))
type(rf_pyfunc_model)

# COMMAND ----------

import mlflow.pyfunc

nn_pyfunc_model = mlflow.pyfunc.load_pyfunc(path=nn_path.replace("dbfs:", "/dbfs"))
type(nn_pyfunc_model)

# COMMAND ----------

# MAGIC %md
# MAGIC Both will implement a predict method.  The `sklearn` model is still of type `sklearn` because this package natively implements this method.

# COMMAND ----------

rf_pyfunc_model.predict(X_test)

# COMMAND ----------

nn_pyfunc_model.predict(X_test)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Pre and Post Processing Code using `pyfunc`
# MAGIC 
# MAGIC A `pyfunc` is a generic python model that can define any model, regardless of the libraries used to train it.  As such, it's defined as a directory structure with all of the dependencies.  It is then "just an object" with a predict method.  Since it makes very few assumptions, it can be deployed using MLflow, Sagemaker, a Spark UDF or in any other environment.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Check out <a href="https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#pyfunc-create-custom" target="_blank">the `pyfunc` documentation for details</a><br>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Check out <a href="https://github.com/mlflow/mlflow/blob/master/docs/source/models.rst#id217" target="_blank">this README for generic example code and integration with `XGBoost`</a>

# COMMAND ----------

# MAGIC %md
# MAGIC Define a model class.

# COMMAND ----------

import mlflow.pyfunc

class AddN(mlflow.pyfunc.PythonModel):

    def __init__(self, n):
        self.n = n

    def predict(self, context, model_input):
        return model_input.apply(lambda column: column + self.n)

# COMMAND ----------

# MAGIC %md
# MAGIC Construct and save the model.

# COMMAND ----------

model_path = userhome+"add_n_model2"
add5_model = AddN(n=5)

mlflow.pyfunc.save_model(dst_path=model_path, python_model=add5_model)

# COMMAND ----------

# MAGIC %md
# MAGIC Load the model in `python_function` format

# COMMAND ----------

loaded_model = mlflow.pyfunc.load_pyfunc(model_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate the model.

# COMMAND ----------

import pandas as pd

model_input = pd.DataFrame([range(10)])
model_output = loaded_model.predict(model_input)

assert model_output.equals(pd.DataFrame([range(5, 15)]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review 
# MAGIC **Question:** How do MLflow projects differ from models?  
# MAGIC **Answer:** The focus of MLflow projects is reproducibility of runs and packaging of code.  MLflow models focuses on various deployment environments.  
# MAGIC 
# MAGIC **Question:** What is a ML model flavor?  
# MAGIC **Answer:** Flavors are a convention that deployment tools can use to understand the model, which makes it possible to write tools that work with models from any ML library without having to integrate each tool with each library.  Instead of having to map each training environment to a deployment environment, ML model flavors manages this mapping for you.
# MAGIC 
# MAGIC **Question:** How do I add pre and post processing logic to my models?  
# MAGIC **Answer:** A model class that extends `mlflow.pyfunc.PythonModel` allows you to have load, preprocessing, and postprocessing logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Production Issues]($./05-Production-Issues ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on MLflow Models?  
# MAGIC **A:** Check out <a href="https://www.mlflow.org/docs/latest/models.html" target="_blank">the MLflow documentation</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>