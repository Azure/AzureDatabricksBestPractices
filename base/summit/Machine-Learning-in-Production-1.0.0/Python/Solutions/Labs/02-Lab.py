# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Grid Search with MLflow

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Load in same Airbnb data and create train/test split.

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating New MLflow Experiment
# MAGIC 
# MAGIC Complete the `experimentPath` code below to create a brand new experiment under your user folder named `02-Lab-GridSearch`.
# MAGIC 
# MAGIC Open this experiment's MLflow UI in a different tab afterwards.

# COMMAND ----------

# ANSWER
import mlflow
from mlflow.exceptions import MlflowException
from  mlflow.tracking import MlflowClient

experimentPath = "/Users/" + username + "/" + "02-Lab-GridSearch"

try:
  experimentID = mlflow.create_experiment(experimentPath)
  print("The experimment can be found at the path `{}` and has an experiment_id of `{}`".format(experimentPath, experimentID))
except MlflowException: 
  print("ERROR: Experiment with path `{}` already exists".format(experimentPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Grid Search 
# MAGIC 
# MAGIC We want to know which combination of hyperparamter values is the most effective! Fill in the code below to perform <a href="https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.GridSearchCV.html#sklearn.model_selection.GridSearchCV" target="_blank"> grid search using sklearn</a> over the 2 hyperparameters we looked at in the 02 notebook, `n_estimators` and `max_depth`.

# COMMAND ----------

# ANSWER
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV

# dictionary containing hyperparameter names and list of values we want to try
parameters = {'n_estimators':[100,1000], 
              'max_depth':[10,15]}

rf = RandomForestRegressor()
grid_rf_model = GridSearchCV(rf, parameters, cv=3)
grid_rf_model.fit(X_train, y_train)

best_rf = grid_rf_model.best_estimator_
for p in parameters:
  print("Best '{}': {}".format(p, best_rf.get_params()[p]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Best Model on MLflow
# MAGIC 
# MAGIC Log the best model as `grid-random-forest-model`, its parameters, and its MSE metric under a run with name `RF-Grid-Search` in our new MLflow experiment.

# COMMAND ----------

# ANSWER
from sklearn.metrics import mean_squared_error

with mlflow.start_run(experiment_id=experimentID, run_name="RF-Grid-Search") as run:
  # Create predictions of X_test using best model
  predictions = best_rf.predict(X_test)
  
  # Log model
  mlflow.sklearn.log_model(best_rf, "grid-random-forest-model")
  
  # Log params
  model_params = best_rf.get_params()
  [mlflow.log_param(p, model_params[p]) for p in parameters]
  
  # Create and log MSE metrics using predictions of X_test and its actual value y_test
  mse = mean_squared_error(y_test, predictions)
  mlflow.log_metric("mse", mse)
  
  runID = run.info.run_uuid
  print("Inside MLflow Run with id {}".format(runID))

# COMMAND ----------

# MAGIC %md
# MAGIC Check on the MLflow UI that the run `RF-Grid-Search` is logged has the best parameter values found by grid search.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the Random Forest Model
# MAGIC 
# MAGIC Fill in the `path` and `run_id` to load the trained and tuned model we just saved.

# COMMAND ----------

# ANSWER
loaded_model = mlflow.sklearn.load_model(path="grid-random-forest-model", run_id=runID)
loaded_model


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>