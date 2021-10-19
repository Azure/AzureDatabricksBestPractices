# Databricks notebook source
# MAGIC %md ![](/files/ONNX/slide_1.png)

# COMMAND ----------

# MAGIC %md ![ONNX Slide 1](/files/ONNX/slide_1.png)

# COMMAND ----------

# MAGIC %md ![ONNX Slide 2](/files/ONNX/slide_2.png)

# COMMAND ----------

# MAGIC %md ![ONNX Slide 3](/files/ONNX/slide_3.png)

# COMMAND ----------

# MAGIC %md ![ONNX Slide 4](/files/ONNX/slide_4.png)

# COMMAND ----------

# MAGIC %md ![ONNX Slide 5](/files/ONNX/slide_5.png)

# COMMAND ----------

# MAGIC %md ![ONNX Slide 6](/files/ONNX/slide_6.png)

# COMMAND ----------

# MAGIC %md ## MLflow Wine Quality Training
# MAGIC 
# MAGIC * Libraries:
# MAGIC   * PyPI package: mlflow 
# MAGIC * Synopsis:
# MAGIC   * Task: Predict subjective quality of the wine based on chemical properties
# MAGIC   * Model: DecisionTreeRegressor
# MAGIC   * Data: Wine quality
# MAGIC   * Logs  model as sklearn (pickle) format
# MAGIC   * Logs a PNG plot artifact

# COMMAND ----------

# MAGIC %md ### Setup

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Install required libraries for ONNX (if not pre-installed on cluster)

# COMMAND ----------

# MAGIC %sh pip install skl2onnx

# COMMAND ----------

# MAGIC %md #### Run helper notebook for widget and logging convenience functions

# COMMAND ----------

# MAGIC %run ./Utils

# COMMAND ----------

# MAGIC %md #### Model parameters can be set using widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("Experiment Name", create_experiment_name("wine"))
dbutils.widgets.text("Max Depth", "5") 
dbutils.widgets.text("Max Leaf Nodes", "32")
dbutils.widgets.dropdown("Use ONNX","yes",["yes","no"])

# COMMAND ----------

experiment_name = dbutils.widgets.get("Experiment Name")
mlflow.set_experiment(experiment_name)

# COMMAND ----------

# MAGIC %md ### Prepare data

# COMMAND ----------

data_path = download_wine_file()

# COMMAND ----------

import pandas as pd
data = pd.read_csv(data_path)
display(data)

# COMMAND ----------

data.describe()

# COMMAND ----------

from sklearn.model_selection import train_test_split

train, test = train_test_split(data)

# The predicted column is "quality" which is a scalar between 0 and 10
train_x = train.drop(["quality"], axis=1)
test_x = test.drop(["quality"], axis=1)
train_y = train[["quality"]]
test_y = test[["quality"]]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train

# COMMAND ----------

max_depth = int(dbutils.widgets.get("Max Depth"))
max_leaf_nodes = int(dbutils.widgets.get("Max Leaf Nodes"))
log_as_onnx = dbutils.widgets.get("Use ONNX") == "yes"

max_depth, max_leaf_nodes, experiment_name, log_as_onnx

# COMMAND ----------

import mlflow.sklearn
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.tree import DecisionTreeRegressor
from mlflow.entities import Metric, Param, RunTag

run = mlflow.start_run()
run_id = run.info.run_id
experiment_id = run.info.experiment_id
print("MLflow:")
print("  run_id:", run_id)
print("  experiment_id:", experiment_id)  

#  Params
print("Parameters:")
print("  max_depth:", max_depth)
print("  max_leaf_nodes:", max_leaf_nodes)

# Create and fit model
dt = DecisionTreeRegressor(max_depth=max_depth, max_leaf_nodes=max_leaf_nodes)
dt.fit(train_x, train_y)

# Metrics
predictions = dt.predict(test_x)
rmse = np.sqrt(mean_squared_error(test_y, predictions))
mae = mean_absolute_error(test_y, predictions)
r2 = r2_score(test_y, predictions)  
print("Metrics:")
print("  rmse:", rmse)
print("  mae:", mae)
print("  r2:", r2)

# Log params, metrics and tags
import time
now = round(time.time())
metrics = [Metric("rmse",rmse, now, 0), Metric("r2", r2, now, 0)]
params = [Param("max_depth", str(max_depth)), 
          Param("max_leaf_nodes", str(max_leaf_nodes))]
mlflow_client.log_batch(run_id, metrics, params, create_version_tags())

# Log Sklearn model
mlflow.sklearn.log_model(dt, "sklearn-model")

# Log ONNX model
if log_as_onnx:
    import mlflow.onnx
    import onnx
    import skl2onnx
    initial_type = [('float_input', skl2onnx.common.data_types.FloatTensorType([None, test_x.shape[1]]))]
    onnx_model = skl2onnx.convert_sklearn(dt, initial_types=initial_type)
    print("onnx_model.type:", type(onnx_model))
    mlflow.onnx.log_model(onnx_model, "onnx-model")
    mlflow.set_tag("onnx_version", onnx.__version__)

# Create and log plot of predicted versus actual
plot_file = "ground_truth_vs_predicted.png"
create_plot_file(test_y, predictions, plot_file)
mlflow.log_artifact(plot_file)

# COMMAND ----------

# MAGIC %md #### Examine the feature importances from the fitted tree.  Alcohol and acidity top the list

# COMMAND ----------

pd.DataFrame({'feature': train_x.columns, 'importance': dt.feature_importances_}).sort_values(by='importance', ascending=False)

# COMMAND ----------

# MAGIC %md #### Take a look at the tree, this can also be logged as an artifact

# COMMAND ----------

from sklearn.tree import plot_tree
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(12, 12))
plot_tree(dt, max_depth=3, fontsize=10)
fig.savefig("decision_tree.png")
mlflow.log_artifact("decision_tree.png")

# COMMAND ----------

# MAGIC %md ### Display run and we can take a look at what we logged

# COMMAND ----------

mlflow.end_run()
display_run_uri(experiment_id, run_id)

# COMMAND ----------

# MAGIC %md ### You can change parameters in the widgets, rerun and compare r2 and rmse