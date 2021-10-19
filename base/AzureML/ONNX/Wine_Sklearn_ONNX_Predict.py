# Databricks notebook source
# MAGIC %md ## MLflow Wine Quality Prediction
# MAGIC * Runs predictions for runs from [Wine_Sklearn_ONNX_Train](https://westus2.azuredatabricks.net/?o=6935536957980197#notebook/1352754433853362).
# MAGIC * Predicts with following flavors:
# MAGIC   * UDF - both DF and SQL for ONNX and sklearn
# MAGIC   * sklearn - [mlflow.sklearn.load_model()](https://mlflow.org/docs/latest/python_api/mlflow.sklearn.html#mlflow.sklearn.load_model)
# MAGIC   * pyfunc - both sklearn and ONXX - [mlflow.pyfunc.load_model()](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#mlflow.pyfunc.load_model)
# MAGIC   * ONNX - [mlflow.onnx.load_model](https://www.mlflow.org/docs/latest/python_api/mlflow.onnx.html#mlflow.onnx.load_model)

# COMMAND ----------

# MAGIC %md #### Install library for ONNX runtime prediction (if not already installed on cluster)

# COMMAND ----------

# MAGIC %sh pip install onnxruntime

# COMMAND ----------

# MAGIC %md #### Run helper notebook for widget convenience functions

# COMMAND ----------

# MAGIC %run ./UtilsPredict

# COMMAND ----------

# MAGIC %md ### Setup widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

exp_name = create_experiment_name("wine")
run_id,experiment_id,experiment_name,run_id,metric,run_mode = create_predict_widgets(exp_name, ["mae","r2","rmse"])
run_id,experiment_id,experiment_name,metric,run_mode

# COMMAND ----------

# MAGIC %md ### Read data

# COMMAND ----------

df = read_wine_data()
df = df.drop("quality")

# COMMAND ----------

# MAGIC %md ### Predict with Sklearn and ONNX flavors

# COMMAND ----------

sklearn_uri = f"runs:/{run_id}/sklearn-model"
onnx_uri = f"runs:/{run_id}/onnx-model"

# COMMAND ----------

# MAGIC %md ##### Single-Node with flavor

# COMMAND ----------

pandas_df = df.toPandas()

# COMMAND ----------

import mlflow.sklearn

model = mlflow.sklearn.load_model(sklearn_uri)
predictions = model.predict(pandas_df)

# COMMAND ----------

import mlflow.onnx
import onnxruntime
import numpy as np

model = mlflow.onnx.load_model(onnx_uri)
sess = onnxruntime.InferenceSession(model.SerializeToString())
input_name = sess.get_inputs()[0].name
predictions = sess.run(None, {input_name: pandas_df.to_numpy().astype(np.float32)})[0]

# COMMAND ----------

# MAGIC %md ##### Single-Node with pyfunc wrapper

# COMMAND ----------

import pandas as pd

model_sklearn = mlflow.pyfunc.load_model(sklearn_uri)
predictions_sklearn = model_sklearn.predict(pandas_df)

model_onnx = mlflow.pyfunc.load_model(onnx_uri)
predictions_onnx = model_onnx.predict(pandas_df)

np.isclose(predictions_sklearn, predictions_onnx['variable'], atol=0.00001).all()

# COMMAND ----------

# MAGIC %md ##### Distributed Scoring with Spark UDF

# COMMAND ----------

udf = mlflow.pyfunc.spark_udf(spark, sklearn_uri)
predictions = df.withColumn("prediction", udf(*df.columns))
display(predictions)

# COMMAND ----------

onnx_udf = mlflow.pyfunc.spark_udf(spark, onnx_uri)
predictions = df.withColumn("prediction", udf(*df.columns))
display(predictions)

# COMMAND ----------

spark.udf.register("predictUDF", udf)
df.createOrReplaceTempView("data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, predictUDF(*) as prediction from data