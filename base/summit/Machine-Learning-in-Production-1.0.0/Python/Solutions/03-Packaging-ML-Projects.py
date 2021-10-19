# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Packaging ML Projects
# MAGIC 
# MAGIC Machine learning projects need to produce both reusable code and reproducable results.  This lesson examines creating, organizing, and packaging machine learning projects with a focus on reproducability and collaborating with a team.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Introduce organizing code into projects
# MAGIC  - Package a basic project with parameters and an environment
# MAGIC  - Run a basic project locally and remotely
# MAGIC  - Design a multi-step workflow with many different components

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### The Case for Packaging
# MAGIC 
# MAGIC There are a number of different reasons why teams need to package their machine learning projects:<br><br>
# MAGIC 
# MAGIC 1. Projects have various libraries dependencies so shipping a machine learning solution involves the environment in which it was built.  MLflow allows for this environment to be a conda environment or docker container.  This means that teams can easily share and publish their code for others to use.
# MAGIC 2. Machine learning projects become increasingly complex as time goes on.  This includes ETL and featurization steps, machine learning models used for pre-processing, and finally the model training itself.
# MAGIC 3. Each component of a machine learning pipeline needs to allow for tracing its lineage.  If there's a failure at some point, tracing the full end-to-end lineage of a model allows for easier debugging.
# MAGIC 
# MAGIC **ML Projects is a specification for how to organize code in a project.**  The heart of this is an **MLproject file,** a YAML specification for the components of the ML project.  This allows for more complex workflows since a project can execute another project, allowing for encapsulation of each stage of a more complex machine learning architecture.  This means that teams can collaborate more easily using this architecture.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-project.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Packaging a Simple Project
# MAGIC 
# MAGIC First we're going to create a simple MLflow project consisting of the following elements:<br><br>
# MAGIC 
# MAGIC 1. MLProject file
# MAGIC 2. Conda environment
# MAGIC 3. Basic code
# MAGIC 
# MAGIC We're going to want to be able to pass parameters into this code so that we can try different hyperparameter options.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a new experiment for this exercise.  Navigate to the UI in another tab.

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

# MAGIC %md-sandbox
# MAGIC First, examine the code we're going to run.  This looks similar to what we ran in the last lesson with the addition of decorators from the `click` library.  This allows us to parameterize our code.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll uncomment out the `__main__` block when we save this code as a Python file.

# COMMAND ----------

import click
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

@click.command()
@click.option("--data_path", default="/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv", type=str)
@click.option("--n_estimators", default=10, type=int)
@click.option("--max_depth", default=20, type=int)
@click.option("--max_features", default="auto", type=str)
def mlflow_rf(data_path, n_estimators, max_depth, max_features):

  with mlflow.start_run() as run:
    # Import the data
    df = pd.read_csv(data_path)
    X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)
    
    # Create model, train it, and create predictions
    rf = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, max_features=max_features)
    rf.fit(X_train, y_train)
    predictions = rf.predict(X_test)

    # Log model
    mlflow.sklearn.log_model(rf, "random-forest-model")
    
    # Log params
    mlflow.log_param("n_estimators", n_estimators)
    mlflow.log_param("max_depth", max_depth)
    mlflow.log_param("max_features", max_features)

    # Log metrics
    mlflow.log_metric("mse", mean_squared_error(y_test, predictions))
    mlflow.log_metric("mae", mean_absolute_error(y_test, predictions))  
    mlflow.log_metric("r2", r2_score(y_test, predictions))  

# if __name__ == "__main__":
#   mlflow_rf() # Note that this does not need arguments thanks to click

# COMMAND ----------

# MAGIC %md
# MAGIC Test that it works using the `click` `CliRunner`, which will execute the code in the same way we expect to.

# COMMAND ----------

from click.testing import CliRunner

runner = CliRunner()
result = runner.invoke(mlflow_rf, ['--n_estimators', 10, '--max_depth', 20], catch_exceptions=True)

assert result.exit_code == 0, "Code failed" # Check to see that it worked

print("Success!")

# COMMAND ----------

# MAGIC %md
# MAGIC Now create a directory to hold our project files.  This will be a unique directory for your username.

# COMMAND ----------

train_path = "/tmp/" + username + "/mlflow-model-training/"

dbutils.fs.rm(train_path, True) # Clears the directory if it already exists
dbutils.fs.mkdirs(train_path)

print("Created directory `{}` to house the project files.".format(train_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Create the `MLproject` file.  This is the heart of an MLflow project.  It includes pointers to the conda environment and a `main` entry point, which is backed by the file `train.py`.

# COMMAND ----------

dbutils.fs.put(train_path + "/MLproject", 
'''
name: Lesson-3-Model-Training

conda_env: conda.yaml

entry_points:
  main:
    parameters:
      data_path: {type: str, default: "/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv"}
      n_estimators: {type: int, default: 10}
      max_depth: {type: int, default: 20}
      max_features: {type: str, default: "auto"}
    command: "python train.py --data_path {data_path} --n_estimators {n_estimators} --max_depth {max_depth} --max_features {max_features}"
'''.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC Create the conda environment.

# COMMAND ----------

dbutils.fs.put(train_path + "/conda.yaml", 
'''
name: tutorial
channels:
  - defaults
dependencies:
  - cloudpickle=0.6.1
  - python=3.6
  - numpy=1.14.3
  - pandas=0.22.0
  - scikit-learn=0.19.1
  - pip:
    - mlflow==0.9.0
'''.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC Now create the code itself.  This is the same as above except for with the `__main__` is included.  Note how there are no arguments passed into `mlflow_rf()` on the final line.  `click` is handling the arguments for us.

# COMMAND ----------

dbutils.fs.put(train_path + "/train.py", 
'''
import click
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

@click.command()
@click.option("--data_path", default="/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv", type=str)
@click.option("--n_estimators", default=10, type=int)
@click.option("--max_depth", default=20, type=int)
@click.option("--max_features", default="auto", type=str)
def mlflow_rf(data_path, n_estimators, max_depth, max_features):

  with mlflow.start_run() as run:
    # Import the data
    df = pd.read_csv(data_path)
    X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)
    
    # Create model, train it, and create predictions
    rf = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, max_features=max_features)
    rf.fit(X_train, y_train)
    predictions = rf.predict(X_test)

    # Log model
    mlflow.sklearn.log_model(rf, "random-forest-model")
    
    # Log params
    mlflow.log_param("n_estimators", n_estimators)
    mlflow.log_param("max_depth", max_depth)
    mlflow.log_param("max_features", max_features)

    # Log metrics
    mlflow.log_metric("mse", mean_squared_error(y_test, predictions))
    mlflow.log_metric("mae", mean_absolute_error(y_test, predictions))  
    mlflow.log_metric("r2", r2_score(y_test, predictions))  

if __name__ == "__main__":
  mlflow_rf() # Note that this does not need arguments thanks to click
'''.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC To summarize, you now have three files: `MLmodel`, `conda.yaml`, and `train.py`

# COMMAND ----------

dbutils.fs.ls(train_path)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Running Projects
# MAGIC 
# MAGIC Now you have the three files we need to run the project, we can trigger the run.  We'll do this in a few different ways:<br><br>
# MAGIC 
# MAGIC 1. On the driver node of our Spark cluster
# MAGIC 2. On a new Spark cluster submitted as a job
# MAGIC 3. Using files backed by github
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This currently relies on environment variables.  See the setup script for details.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Now run the experiment.  This command will execute against the driver node of a Spark cluster, though it could be running locally or on a VM.
# MAGIC 
# MAGIC First set the experiment using the `experimentPath` defined earlier.  Prepend `/dbfs` to the file path, which allows the cluster's file system to access DBFS.  Then, pass your parameters.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This will take a few minutes to work the first time.  Subsequent runs are faster.

# COMMAND ----------

import mlflow

mlflow.set_experiment(experiment_name=experimentPath)

mlflow.projects.run("/dbfs" + train_path,
  parameters={
    "data_path": "/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv",
    "n_estimators": 10,
    "max_depth": 20,
    "max_features": "auto"
})

# COMMAND ----------

# MAGIC %md
# MAGIC Now that it's working, experiment with other parameters.  Note how much faster it runs the second time.

# COMMAND ----------

mlflow.projects.run("/dbfs" + train_path,
  parameters={
    "data_path": "/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv",
    "n_estimators": 1000,
    "max_depth": 10,
    "max_features": "log2"
})

# COMMAND ----------

# MAGIC %md
# MAGIC How did the new model do?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Now try executing this code against a new Databricks cluster.  This needs to define cluster specifications.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/>  <a href="https://docs.databricks.com/api/latest/clusters.html" target="_blank">See the clusters API docs</a> to see how to define cluster specifications.

# COMMAND ----------

clusterspecs = {
    "num_workers": 2,
    "spark_version": "5.3.x-cpu-ml-scala2.11",
    "node_type_id": "Standard_DS3_v2",
    "driver_node_type_id": "Standard_DS3_v2",
}

mlflow.projects.run(
  uri="/dbfs" + train_path,
  parameters={
    "data_path": "/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv",
    "n_estimators": 1500,
    "max_depth": 5,
    "max_features": "sqrt"
},
  mode="databricks",
  cluster_spec=clusterspecs
)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, run this example, which is <a href="https://github.com/mlflow/mlflow-example" target="_blank">a project backed by github.</a>

# COMMAND ----------

mlflow.run(
  uri="https://github.com/mlflow/mlflow-example",
  parameters={'alpha':0.4}
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Multi-Step Workflows
# MAGIC 
# MAGIC Now that we can package projects and run them in their environment, let's look at how how we can make workflows consisting of multiple steps.  The underlying idea that makes this possible is that **runs can recursively call other runs.**  This means that steps in a machine learning pipeline can be isolated.  There are three general architectures to consider:<br><br>
# MAGIC 
# MAGIC 1. One driver project calls other entry points in that same project
# MAGIC 2. One driver project calls other projects 
# MAGIC 3. One project calls another project as its final step
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlproject-architecture1.png" style="height: 250px; margin: 20px"/></div>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlproject-architecture2.png" style="height: 250px; margin: 20px"/></div>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlproject-architecture3.png" style="height: 250px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC Create a preprocessing project that reads and saves the data to a new location.  This is just a minimum viable product for a working pipeline.  In practice, this would likely include an ETL stage.
# MAGIC 
# MAGIC Do this by first creating a new directory for it to live.

# COMMAND ----------

load_path = "/tmp/" + username + "/mlflow-data-loading/"

dbutils.fs.rm(load_path, True) # Clears the directory if it already exists
dbutils.fs.mkdirs(load_path)

print("Created directory `{}` to house the project files.".format(load_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Now create another `MLproject` file.

# COMMAND ----------

dbutils.fs.put(load_path + "/MLproject", 
'''
name: Lesson-3-Data-Loading

conda_env: conda.yaml

entry_points:
  main:
    parameters:
      data_input_path: {type: str, default: "/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv"}
    command: "python load.py --data_input_path {data_input_path}"
'''.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC Create the environment as well.

# COMMAND ----------

dbutils.fs.put(load_path + "/conda.yaml", 
'''
name: tutorial
channels:
  - defaults
dependencies:
  - cloudpickle=0.6.1
  - python=3.6
  - pip:
    - mlflow==0.8.0
'''.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC You can test the code below.  It simply takes an input path and logs the related data as an MLflow artifact.

# COMMAND ----------

import click
import mlflow

# @click.command()
# @click.option("--data_input_path", default="/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv", type=str)
def data_load(data_input_path):

  with mlflow.start_run() as run:
    # Log the data
    mlflow.log_artifact(data_input_path, "data-csv-dir")

if __name__ == "__main__":
  data_load("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")
  
dbutils.fs.put(load_path + "/load.py", 
'''
import click
import mlflow

@click.command()
@click.option("--data_input_path", default="/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv", type=str)
def data_load(data_input_path):

  with mlflow.start_run() as run:
    # Log the data
    mlflow.log_artifact(data_input_path, "data-csv-dir")

if __name__ == "__main__":
  data_load()
'''.strip())

dbutils.fs.ls(load_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Now run the data loading code.

# COMMAND ----------

submitted_run = mlflow.projects.run("/dbfs/"+load_path,
  parameters={
    "data_input_path": "/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv"
  })

# COMMAND ----------

# MAGIC %md
# MAGIC Get the artifact URI from the MLflow client.

# COMMAND ----------

artifact_uri = mlflow.tracking.MlflowClient().get_run(submitted_run.run_id).info.artifact_uri

dbutils.fs.ls(artifact_uri+"/data-csv-dir")

# COMMAND ----------

# MAGIC %md
# MAGIC Run the training code using the URI.

# COMMAND ----------

mlflow.projects.run("/dbfs" + train_path,
  parameters={
    "data_path": artifact_uri.replace("dbfs:", "/dbfs")+"/data-csv-dir/airbnb-cleaned-mlflow.csv"
})

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Clean up the workspace by deleting the experiment.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This will delete all of your runs from this notebook.

# COMMAND ----------

from  mlflow.tracking import MlflowClient

client = MlflowClient()
client.delete_experiment(experimentID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC  - Introduce organizing code into projects
# MAGIC  - Package a basic project with parameters and an environment
# MAGIC  - Run a basic project locally and remotely
# MAGIC  - Design a multi-step workflow with many different components
# MAGIC 
# MAGIC **Question:** Why is packaging important?  
# MAGIC **Answer:** Packaging not only manages your code but the environment in which it was run.  This enviornment can be a Conda or Docker environment.  This ensures that you have reproducable code and models that can be used in a number of downstream environments.
# MAGIC 
# MAGIC **Question:** What are the core components of MLflow projects?  
# MAGIC **Answer:** An MLmodel specifies the project components using YAML.  The environment file contains specifics about the environment.  The code itself contains the steps to create a model or process data.
# MAGIC 
# MAGIC **Question:** What code can I run and where can I run it?  
# MAGIC **Answer:** Arbitrary code can be run in any number of different languages.  It can be run locally or remotely, whether on a remote VM, Spark cluster, or submitted as a Databricks job.
# MAGIC 
# MAGIC **Question:** How can I manage a pipeline using MLflow?  
# MAGIC **Answer:** Multi-step workflows chain together multiple MLflow jobs, allowing for better encapsulation of steps such as fetching data, ETL, machine learning as a preprocessing step, and the training of the final model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Model Management]($./04-Model-Management ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on MLflow Projects?  
# MAGIC **A:** Check out the <a href="https://www.mlflow.org/docs/latest/projects.html" target="_blank">MLflow docs</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>