# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Experiment Tracking
# MAGIC 
# MAGIC The machine learning lifecyle involves training multiple algorithms using different hyperparameters and libraries, all with different performance results and trained models.  This lesson explores tracking those experiments to organize the machine learning lifecyle.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Introduce tracking ML experiments in MLflow
# MAGIC  - Log an experiment and explore the results in the UI
# MAGIC  - Record parameters, metrics, and a model
# MAGIC  - Query past runs programatically

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Tracking Experiments with MLflow
# MAGIC 
# MAGIC Over the course of the machine learning lifecycle, data scientists test many different models from various libraries with different hyperparemeters.  Tracking these various results poses an organizational challenge.  In brief, storing experiements, results, models, supplementary artifacts, and code creates significant challenges in the machine learning lifecycle.
# MAGIC 
# MAGIC MLflow Tracking is a logging API specific for machine learning and agnostic to libraries and environments that do the training.  It is organized around the concept of **runs**, which are executions of data science code.  Runs are aggregated into **experiments** where many runs can be a part of a given experiment and an MLflow server can host many experiments.
# MAGIC 
# MAGIC Each run can record the following information:<br><br>
# MAGIC 
# MAGIC - **Parameters:** Key-value pairs of input parameters such as the number of trees in a random forest model
# MAGIC - **Metrics:** Evaluation metrics such as RMSE or Area Under the ROC Curve
# MAGIC - **Artifacts:** Arbitrary output files in any format.  This can include images, pickled models, and data files
# MAGIC - **Source:** The code that originally ran the experiement
# MAGIC 
# MAGIC MLflow tracking also serves as a **model registry** so tracked models can easily be stored and, as necessary, deployed into production.
# MAGIC 
# MAGIC Experiments can be tracked using libraries in Python, R, and Java as well as by using the CLI and REST calls.  This course will use Python, though the majority of MLflow funcionality is also exposed in these other APIs.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember to attach your notebook to a cluster. Click <b>Detached</b> in the upper left hand corner and then select your preferred cluster.
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Experiment Logging and UI
# MAGIC 
# MAGIC MLflow is an open source software project developed by Databricks available to developers regardless of which platform they are using.  Databricks hosts MLflow for you, which reduces deployment configuration and adds security benefits.  As of February 2019, MLflow is in public preview and is therefore accessible on all Databricks workspaces.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See <a href="https://mlflow.org/docs/latest/quickstart.html#" target="_blank">the MLflow quickstart guide</a> for details on setting up MLflow locally or on your own server.

# COMMAND ----------

# MAGIC %md
# MAGIC MLflow has number of dependencies, which have been installed for you for this class.  If you are running this code on a different cluster from what has been provisioned for you, uncomment and run the following code to install these libraries:
# MAGIC 
# MAGIC  - mlflow==0.9.0
# MAGIC  - scikit-learn==0.20.2
# MAGIC  - matplotlib==3.0.2

# COMMAND ----------

# dbutils.library.installPyPI("mlflow==0.9.0")
# dbutils.library.installPyPI("scikit-learn==0.20.2")
# dbutils.library.installPyPI("matplotlib==3.0.2")

# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Import a dataset of Airbnb listings and featurize the data.  We'll use this to train a model.

# COMMAND ----------

import pandas as pd

df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a train/test split.

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

# COMMAND ----------

# MAGIC %md
# MAGIC Create your own experiment.  Experiments allow you to aggregate runs.

# COMMAND ----------

import mlflow
from mlflow.exceptions import MlflowException
from  mlflow.tracking import MlflowClient

experimentPath = "/Users/" + username + "/experiment-L2"

try:
  experimentID = mlflow.create_experiment(experimentPath)
except MlflowException: # if experiment is already created
  experimentID = MlflowClient().get_experiment_by_name(experimentPath).experiment_id
  mlflow.set_experiment(experimentPath)

print("The experiment can be found at the path `{}` and has an experiment_id of `{}`".format(experimentPath, experimentID))

# COMMAND ----------

# MAGIC %md
# MAGIC **Navigate to the MLflow UI by clicking on the `Home` button on the lefthand side of the screen and navigating to `/Users/ < your username > /experiment-lesson2`.**  Open this in a second tab.

# COMMAND ----------

# MAGIC %md
# MAGIC Log a basic experiment by doing the following:<br><br>
# MAGIC 
# MAGIC 1. Start an experiment using `mlflow.start_run()` and passing it `experimentID` and a name for the run
# MAGIC 2. Train your model
# MAGIC 3. Log the model using `mlflow.sklearn.log_model()`
# MAGIC 4. Log the error using `mlflow.log_metric()`
# MAGIC 5. Print out the run id using `run.info.run_uuid`

# COMMAND ----------

import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

with mlflow.start_run(experiment_id=experimentID, run_name="Basic RF Experiment") as run:
  # Create model, train it, and create predictions
  rf = RandomForestRegressor()
  rf.fit(X_train, y_train)
  predictions = rf.predict(X_test)
  
  # Log model
  mlflow.sklearn.log_model(rf, "random-forest-model")
  
  # Create metrics
  mse = mean_squared_error(y_test, predictions)
  print("  mse: {}".format(mse))
  
  # Log metrics
  mlflow.log_metric("mse", mse)
  
  runID = run.info.run_uuid
  print("Inside MLflow Run with id {}".format(runID))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Examine the results in the UI.  Look for the following:<br><br>
# MAGIC 
# MAGIC 1. The `Experiment ID`
# MAGIC 2. The artifact location.  This is where the artificats are stored in DBFS, which is backed by {{#amazon}}an Amazon S3 bucket{{/amazon}}{{#azure}}the Azure blob store{{/azure}}
# MAGIC 3. The time the run was executed.  **Click this to see more information on the run.**
# MAGIC 4. The code that executed the run.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-ui-lesson2.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC After clicking on the time of the run, take a look at the following:<br><br>
# MAGIC 
# MAGIC 1. The Run ID will match what we printed above
# MAGIC 2. The model that we saved, included a picked version of the model as well as the Conda environment and the `MLmodel` file, which will be discussed in the next lesson.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-ui-lesson2b.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC Now take a look at the directory structure backing this experiment.  This allows you to retreive artifacts.

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks/mlflow/{}".format(experimentID))

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the contents of `random-forest-model`, which match what we see in the UI.

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks/mlflow/{}/{}/artifacts/random-forest-model".format(experimentID, runID))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters, Metrics, and Artifacts
# MAGIC 
# MAGIC But wait, there's more!  In the last example, you logged the run name, an evaluation metric, and your model itself as an artifact.  Now let's log parameters, multiple metrics, and other artifacts including the feature importances.
# MAGIC 
# MAGIC First, create a function to perform this.

# COMMAND ----------

def log_rf(experimentID, run_name, params, X_train, X_test, y_train, y_test):
  import os
  import matplotlib.pyplot as plt
  import mlflow.sklearn
  import seaborn as sns
  from sklearn.ensemble import RandomForestRegressor
  from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

  with mlflow.start_run(experiment_id=experimentID, run_name=run_name) as run:
    # Create model, train it, and create predictions
    rf = RandomForestRegressor(**params)
    rf.fit(X_train, y_train)
    predictions = rf.predict(X_test)

    # Log model
    mlflow.sklearn.log_model(rf, "random-forest-model")

    # Log params
    [mlflow.log_param(param, value) for param, value in params.items()]

    # Create metrics
    mse = mean_squared_error(y_test, predictions)
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)
    print("  mse: {}".format(mse))
    print("  mae: {}".format(mae))
    print("  R2: {}".format(r2))

    # Log metrics
    mlflow.log_metric("mse", mse)
    mlflow.log_metric("mae", mae)  
    mlflow.log_metric("r2", r2)  
    
    # Create feature importance
    importance = pd.DataFrame(list(zip(df.columns, rf.feature_importances_)), columns=["Feature", "Importance"]).sort_values("Importance", ascending=False)
    outdir = "/tmp/" + username
    if not os.path.exists(outdir):
      os.mkdir(outdir)
    csvPath = outdir + "/feature-importance.csv"
    importance.to_csv(csvPath, index=False)
    mlflow.log_artifact(csvPath)

    # Create and lot plot
    fig, ax = plt.subplots()

    sns.residplot(predictions, y_test, lowess=True)
    plt.xlabel("Predicted values for Price ($)")
    plt.ylabel("Residual")
    plt.title("Residual Plot")

    figPath = "/tmp/" + username + "/residuals.png" 
    fig.savefig(figPath)

    mlflow.log_artifact(figPath)

    display(fig)
    return run.info.run_uuid

# COMMAND ----------

# MAGIC %md
# MAGIC Run with new parameters.

# COMMAND ----------

params = {
  "n_estimators": 100,
  "max_depth": 5,
  "random_state": 42
}

log_rf(experimentID, "Second Run", params, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC Check the UI to see how this appears.  Take a look at the artifact to see where the plot was saved.
# MAGIC 
# MAGIC Now, run a third run.

# COMMAND ----------

params_1000_trees = {
  "n_estimators": 1000,
  "max_depth": 10,
  "random_state": 42
}

log_rf(experimentID, "Third Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Querying Past Runs
# MAGIC 
# MAGIC You can query past runs programatically in order to use this data back in Python.  The pathway to doing this is an `MlflowClient` object. 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can also set tags for runs using `client.set_tag(run.info.run_uuid, "tag_key", "tag_value")`

# COMMAND ----------

from  mlflow.tracking import MlflowClient

client = MlflowClient()

# COMMAND ----------

# MAGIC %md
# MAGIC You can list all active MLflow experiments using the `.list_experiments()` method.

# COMMAND ----------

client.list_experiments()

# COMMAND ----------

# MAGIC %md
# MAGIC Now list all the runs for your experiment using `.list_run_infos()`, which takes your experiment id as a parameter.

# COMMAND ----------

client.list_run_infos(experimentID)

# COMMAND ----------

# MAGIC %md
# MAGIC Pull out a few fields and create a Pandas DataFrame with it.

# COMMAND ----------

runs = pd.DataFrame([(run.run_uuid, run.start_time, run.artifact_uri) for run in client.list_run_infos(experimentID)])
runs.columns = ["run_uuid", "start_time", "artifact_uri"]

display(runs)

# COMMAND ----------

# MAGIC %md
# MAGIC Pull the last run and take a look at the associated artifacts.

# COMMAND ----------

last_run = runs.sort_values("start_time", ascending=False).iloc[0]
dbutils.fs.ls(last_run["artifact_uri"]+"/random-forest-model/")

# COMMAND ----------

# MAGIC %md
# MAGIC Return the evaluation metrics for the last run.

# COMMAND ----------

client.get_run(last_run.run_uuid).data.metrics

# COMMAND ----------

# MAGIC %md
# MAGIC Reload the model and take a look at the feature importance.

# COMMAND ----------

import mlflow.sklearn

model = mlflow.sklearn.load_model(path="random-forest-model", run_id=last_run["run_uuid"]) # Path must match the argument given in the log_model command
model.feature_importances_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What can MLflow Tracking log?  
# MAGIC **Answer:** MLflow can log the following:
# MAGIC - **Parameters:** inputs to a model
# MAGIC - **Metrics:** the performance of the model
# MAGIC - **Artifacts:** any object including data, models, and images
# MAGIC - **Source:** the original code, including the commit hash if linked to git
# MAGIC 
# MAGIC **Question:** How do you log experiments?  
# MAGIC **Answer:** Experiments are logged by first creating a run and using the logging methods on that run object (e.g. `run.log_param("MSE", .2)`).
# MAGIC 
# MAGIC **Question:** Where do logged artifacts get saved?  
# MAGIC **Answer:** Logged artifacts are saved in a directory of your choosing.  On Databricks, this would be DBFS, or the Databricks File System, which backed by a blob store.
# MAGIC 
# MAGIC **Question:** How can I query past runs?  
# MAGIC **Answer:** This can be done using an `MlflowClient` object.  This allows you do everything you can within the UI programatically so you never have to step outside of your programming environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Packaging ML Projects]($./03-Packaging-ML-Projects ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find the MLflow docs?  
# MAGIC **A:** <a href="https://www.mlflow.org/docs/latest/index.html" target="_blank">You can find the docs here.</a>
# MAGIC 
# MAGIC **Q:** What is a good general resource for machine learning?  
# MAGIC **A:** <a href="https://www-bcf.usc.edu/~gareth/ISL/" target="_blank">_An Introduction to Statistical Learning_</a> is a good starting point for the themes and basic approaches to machine learning.
# MAGIC 
# MAGIC **Q:** Where can I find out more information on machine learning with Spark?  
# MAGIC **A:** Check out the Databricks blog <a href="https://databricks.com/blog/category/engineering/machine-learning" target="_blank">dedicated to machine learning</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>