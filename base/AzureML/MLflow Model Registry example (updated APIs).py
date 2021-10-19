# Databricks notebook source
# MAGIC %md # Overview
# MAGIC The MLflow Model Registry component is a centralized model store, set of APIs, and UI, to collaboratively manage the full lifecycle of an MLflow Model. It provides model lineage (which MLflow Experiment and Run produced the model), model versioning, stage transitions (e.g. from staging to production), annotations (e.g. with comments, tags), and deployment management (e.g. which production jobs have requested a specific model version).
# MAGIC 
# MAGIC For more information about the MLflow Model Registry, including detailed API references, see https://mlflow.org/docs/latest/registry.html and https://mlflow.org/docs/latest/model-registry.html.
# MAGIC 
# MAGIC ##### In this notebook, you will use each of the MLflow Model Registry's components to develop and manage a production machine learning application. This example notebook covers the following topics:
# MAGIC 
# MAGIC - **Tracking and logging models with MLflow**
# MAGIC 
# MAGIC - **Registering models with the MLflow Model Registry**
# MAGIC 
# MAGIC - **Describing models and making stage transitions**
# MAGIC 
# MAGIC - **Integrating Registered Models with production applications**
# MAGIC 
# MAGIC - **Searching and discovering models in the MLflow Model Registry**
# MAGIC 
# MAGIC - **Archiving and deleting models**

# COMMAND ----------

# MAGIC %md # Setup instructions
# MAGIC 
# MAGIC The following setup procedure is required in order to run the example notebook:
# MAGIC 
# MAGIC 1. Start a Databricks cluster with the Databricks Machine Learning Runtime (MLR) version 6.6 or later. Python 3 is recommended.
# MAGIC 
# MAGIC 2. Install MLflow version 1.7.0 (`mlflow==1.7.0`) as a cluster library from PyPI.
# MAGIC 
# MAGIC 3. Attach this example notebook to your cluster.

# COMMAND ----------

# MAGIC %md # Machine learning application: Forecasting wind power
# MAGIC 
# MAGIC In this notebook, you will use the MLflow Model Registry to build a machine learning application that forecasts the daily power output of a [wind farm](https://en.wikipedia.org/wiki/Wind_farm). Wind farm power output depends on weather conditions: generally, more energy is produced at higher wind speeds. Accordingly, the machine learning models used in the notebook predict power output based on weather forecasts with three features: `wind direction`, `wind speed`, and `air temperature`.

# COMMAND ----------

# MAGIC %md *This notebook uses altered data from the [National WIND Toolkit dataset](https://www.nrel.gov/grid/wind-toolkit.html) provided by NREL, which is publicly available and cited as follows:*
# MAGIC 
# MAGIC *Draxl, C., B.M. Hodge, A. Clifton, and J. McCaa. 2015. Overview and Meteorological Validation of the Wind Integration National Dataset Toolkit (Technical Report, NREL/TP-5000-61740). Golden, CO: National Renewable Energy Laboratory.*
# MAGIC 
# MAGIC *Draxl, C., B.M. Hodge, A. Clifton, and J. McCaa. 2015. "The Wind Integration National Dataset (WIND) Toolkit." Applied Energy 151: 355366.*
# MAGIC 
# MAGIC *Lieberman-Cribbin, W., C. Draxl, and A. Clifton. 2014. Guide to Using the WIND Toolkit Validation Code (Technical Report, NREL/TP-5000-62595). Golden, CO: National Renewable Energy Laboratory.*
# MAGIC 
# MAGIC *King, J., A. Clifton, and B.M. Hodge. 2014. Validation of Power Output for the WIND Toolkit (Technical Report, NREL/TP-5D00-61714). Golden, CO: National Renewable Energy Laboratory.*

# COMMAND ----------

# MAGIC %md ## Load the dataset
# MAGIC 
# MAGIC The following cells load a dataset containing weather data and power output information for a wind farm in the United States. The dataset contains `wind direction`, `wind speed`, and `air temperature` features sampled every six hours (once at `00:00`, once at `08:00`, and once at `16:00`), as well as daily aggregate power output (`power`), over several years.

# COMMAND ----------

import pandas as pd
wind_farm_data = pd.read_csv("https://github.com/dbczumar/model-registry-demo-notebook/raw/master/dataset/windfarm_data.csv", index_col=0)

def get_training_data():
  training_data = pd.DataFrame(wind_farm_data["2014-01-01":"2018-01-01"])
  X = training_data.drop(columns="power")
  y = training_data["power"]
  return X, y

def get_validation_data():
  validation_data = pd.DataFrame(wind_farm_data["2018-01-01":"2019-01-01"])
  X = validation_data.drop(columns="power")
  y = validation_data["power"]
  return X, y

def get_weather_and_forecast():
  format_date = lambda pd_date : pd_date.date().strftime("%Y-%m-%d")
  today = pd.Timestamp('today').normalize()
  week_ago = today - pd.Timedelta(days=5)
  week_later = today + pd.Timedelta(days=5)
  
  past_power_output = pd.DataFrame(wind_farm_data)[format_date(week_ago):format_date(today)]
  weather_and_forecast = pd.DataFrame(wind_farm_data)[format_date(week_ago):format_date(week_later)]
  if len(weather_and_forecast) < 10:
    past_power_output = pd.DataFrame(wind_farm_data).iloc[-10:-5]
    weather_and_forecast = pd.DataFrame(wind_farm_data).iloc[-10:]

  return weather_and_forecast.drop(columns="power"), past_power_output["power"]

# COMMAND ----------

# MAGIC %md Display a sample of the data for reference.

# COMMAND ----------

wind_farm_data["2019-01-01":"2019-01-14"]

# COMMAND ----------

# MAGIC %md # Train a power forecasting model and track it with MLflow
# MAGIC 
# MAGIC The following cells train a neural network in Keras to predict power output based on the weather features in the dataset. MLflow is used to track the model's hyperparameters, performance metrics, source code, and artifacts.

# COMMAND ----------

# MAGIC %md Define a power forecasting model in Keras.

# COMMAND ----------

def train_keras_model(X, y):
  import keras
  from keras.models import Sequential
  from keras.layers import Dense

  model = Sequential()
  model.add(Dense(100, input_shape=(X_train.shape[-1],), activation="relu", name="hidden_layer"))
  model.add(Dense(1))
  model.compile(loss="mse", optimizer="adam")

  model.fit(X_train, y_train, epochs=100, batch_size=64, validation_split=.2)
  return model

# COMMAND ----------

# MAGIC %md Train the model and use MLflow to track its parameters, metrics, artifacts, and source code.

# COMMAND ----------

import mlflow
import mlflow.keras

X_train, y_train = get_training_data()

with mlflow.start_run():
  # Automatically capture the model's parameters, metrics, artifacts,
  # and source code with the `autolog()` function
  mlflow.keras.autolog()
  
  train_keras_model(X_train, y_train)
  run_id = mlflow.active_run().info.run_id

# COMMAND ----------

# MAGIC %md # Register the model with the MLflow Model Registry
# MAGIC 
# MAGIC Now that a forecasting model has been trained and tracked with MLflow, the next step is to register it with the MLflow Model Registry. You can register and manage models using the MLflow UI (Workflow 1) or the MLflow API (Workflow 2).
# MAGIC 
# MAGIC Follow the instructions for your preferred workflow (UI or API) to register your forecasting model, add rich model descriptions, and perform stage transitions.

# COMMAND ----------

# MAGIC %md ## Workflow 1: Register the model via the MLflow UI

# COMMAND ----------

# MAGIC %md ### Create a new Registered Model
# MAGIC 
# MAGIC First, navigate to the MLflow Runs Sidebar by clicking the `Runs` icon in the Databricks Notebook UI.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/runs_sidebar_icon.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="600">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC Next, locate the MLflow Run corresponding to the Keras model training session, and open it in the MLflow Run UI by clicking the `View Run Detail` icon.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/runs_sidebar_opened.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="500">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC In the MLflow UI, scroll down to the `Artifacts` section and click on the directory named `model`. Click on the `Register Model` button that appears.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/mlflow_ui_register_model.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="1000">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC Then, select `Create New Model` from the drop-down menu, and input the following model name: `power-forecasting-model`. Finally, click `Register`. This registers a new model called `power-forecasting-model` and creates a new model version: `Version 1`.
# MAGIC 
# MAGIC **Note: If a model named `power-forecasting-model` already exists, try adding a unique string - such as your username - to the end (e.g., `power-forecasting-model-corey-zumar`)**
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/register_model_confirm.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="600">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC After a few moments, the MLflow UI displays a link to the new registered model. Follow this link to open the new model version in the MLflow Model Registry UI.

# COMMAND ----------

# MAGIC %md ### Explore the Model Registry UI
# MAGIC 
# MAGIC The Model Version page in the MLflow Model Registry UI provides information about `Version 1` of the registered forecasting model, including its author, creation time, and its current stage.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/registry_version_page.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="600">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC The Model Version page also provides a `Source Run` link, which opens the MLflow Run that was used to create the model in the MLflow Run UI. From the MLflow Run UI, you can access the `Source Notebook Link` to view a snapshot of the Databricks Notebook that was used to train the model.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/source_run_link.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="600">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/source_notebook_link.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="600">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC To navigate back to the MLflow Model Registry, click the `Models` icon in the Databricks Workspace Sidebar. The resulting MLflow Model Registry home page displays a list of all the registered models in your Databricks Workspace, including their versions and stages.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/registry_icon_sidebar.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="100">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC Select the `power-forecasting-model` link to open the Registered Model page, which displays all of the versions of the forecasting model.

# COMMAND ----------

# MAGIC %md ### Add model descriptions
# MAGIC 
# MAGIC You can add descriptions to Registered Models as well as Model Versions: 
# MAGIC * Model Version descriptions are useful for detailing the unique attributes of a particular Model Version (e.g., the methodology and algorithm used to develop the model). 
# MAGIC * Registered Model descriptions are useful for recording information that applies to multiple model versions (e.g., a general overview of the modeling problem and dataset).

# COMMAND ----------

# MAGIC %md Add a high-level description to the registered power forecasting model by clicking the `Edit Description` icon, entering the following description, and clicking `Save`:
# MAGIC 
# MAGIC ```
# MAGIC This model forecasts the power output of a wind farm based on weather data. The weather data consists of three features: wind speed, wind direction, and air temperature.
# MAGIC ```
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/model_description.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="800">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md Next, click the `Version 1` link from the Registered Model page to navigate back to the Model Version page. Then, add a model version description with information about the model architecture and machine learning framework; click the `Edit Description` icon, enter the following description, and click `Save`:
# MAGIC ```
# MAGIC This model version was built using Keras. It is a feed-forward neural network with one hidden layer.
# MAGIC ```
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/model_version_description.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="800">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md ### Perform a model stage transition
# MAGIC 
# MAGIC The MLflow Model Registry defines several model stages: `None`, `Staging`, `Production`, and `Archived`. Each stage has a unique meaning. For example, `Staging` is meant for model testing, while `Production` is for models that have completed the testing or review processes and have been deployed to applications. 
# MAGIC 
# MAGIC Users with appropriate permissions can transition models between stages. In private preview, any user can transition a model to any stage. In the near future, administrators in your organization will be able to control these permissions on a per-user and per-model basis.
# MAGIC 
# MAGIC If you have permission to transition a model to a particular stage, you can make the transition directly. If you do not have permission, you can request a stage transition from another user.

# COMMAND ----------

# MAGIC %md Click the `Stage` button to display the list of available model stages and your available stage transition options. Select `Transition to -> Production` and press `OK` in the stage transition confirmation window to transition the model to `Production`.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/stage_transition_prod.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="600">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/confirm_transition.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="600">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC After the model version is transitioned to `Production`, the current stage is displayed in the UI, and an entry is added to the activity log to reflect the transition.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/stage_production.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="800">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/activity_production.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="800">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md The MLflow Model Registry allows multiple model versions to share the same stage. When referencing a model by stage, the Model Registry uses the latest model version (the model version with the largest version ID). The Registered Model page displays all of the versions of a particular model.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/model_registry_versions.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="800">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md ### Define the model's name programmatically
# MAGIC 
# MAGIC Now that the model has been registered and transitioned to `Production`, future sections of the notebook will reference it using MLflow's programmatic APIs. In order to run these sections, define the Registered Model's name in the cell below.

# COMMAND ----------

model_name = "ravi-power-forecasting-model" # Replace this with the name of your Registered Model, if necessary.

# COMMAND ----------

# MAGIC %md ### Next, navigate to the "Integrate the model with the forecasting application" section of the quickstart.

# COMMAND ----------

# MAGIC %md ## Workflow 2: Register the model via the MLflow API

# COMMAND ----------

# MAGIC %md ### Create a new Registered Model
# MAGIC 
# MAGIC The following cells use the `mlflow.register_model()` function to create a new Registered Model whose name begins with the string `power-forecasting-model`. This also creates a new Model Version (e.g., `Version 1` of `power-forecasting-model-...`).

# COMMAND ----------

def get_model_name():
  try:
    import re
    username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').split("@")[0]
    username = re.sub("[^a-z0-9]", "", username)
  except Exception as e:
    raise e
    import uuid
    username = uuid.uuid4().hex[:10]
  return "power-forecasting-model-{}".format(username)
    
model_name = get_model_name()

# COMMAND ----------

import mlflow

# The default path where the MLflow autologging function stores the Keras model
artifact_path = "model"
model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=run_id, artifact_path=artifact_path)

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

print(model_details)

# COMMAND ----------

# MAGIC %md After creating a model version, it may take a short period of time to become ready. Certain operations, such as model stage transitions, require the model to be in the `READY` state. Other operations, such as adding a description or fetching model details, can be performed before the model version is ready (e.g., while it is in the `PENDING_REGISTRATION` state).
# MAGIC 
# MAGIC The following cell uses the `MlflowClient.get_model_version_details()` function to wait until the model is ready.

# COMMAND ----------

import time
from mlflow.tracking.client import MlflowClient
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus

def wait_until_ready(model_name, model_version):
  client = MlflowClient()
  for _ in range(10):
    model_version_details = client.get_model_version(
      name=model_name,
      version=model_version,
    )
    status = ModelVersionStatus.from_string(model_version_details.status)
    print("Model status: %s" % ModelVersionStatus.to_string(status))
    if status == ModelVersionStatus.READY:
      break
    time.sleep(1)
  
wait_until_ready(model_details.name, model_details.version)

# COMMAND ----------

# MAGIC %md ### Add model descriptions
# MAGIC 
# MAGIC You can add descriptions to Registered Models as well as Model Versions: 
# MAGIC * Model Version descriptions are useful for detailing the unique attributes of a particular Model Version (e.g., the methodology and algorithm used to develop the model). 
# MAGIC * Registered Model descriptions are useful for recording information that applies to multiple model versions (e.g., a general overview of the modeling problem and dataset).

# COMMAND ----------

# MAGIC %md Add a high-level description to the Registered Model, including the machine learning problem and dataset.

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()
client.update_registered_model(
  name=model_details.name,
  description="This model forecasts the power output of a wind farm based on weather data. The weather data consists of three features: wind speed, wind direction, and air temperature."
)

# COMMAND ----------

# MAGIC %md Add a model version description with information about the model architecture and machine learning framework.

# COMMAND ----------

client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description="This model version was built using Keras. It is a feed-forward neural network with one hidden layer."
)

# COMMAND ----------

# MAGIC %md ### Perform a model stage transition
# MAGIC 
# MAGIC The MLflow Model Registry defines several model stages: `None`, `Staging`, `Production`, and `Archived`. Each stage has a unique meaning. For example, `Staging` is meant for model testing, while `Production` is for models that have completed the testing or review processes and have been deployed to applications. 
# MAGIC 
# MAGIC Users with appropriate permissions can transition models between stages. In private preview, any user can transition a model to any stage. In the near future, administrators in your organization will be able to control these permissions on a per-user and per-model basis.
# MAGIC 
# MAGIC If you have permission to transition a model to a particular stage, you can make the transition directly by using the `MlflowClient.update_model_version()` function. If you do not have permission, you can request a stage transition using the REST API; for example:
# MAGIC 
# MAGIC ```
# MAGIC %sh curl -i -X POST -H "X-Databricks-Org-Id: <YOUR_ORG_ID>" -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>" https://<YOUR_DATABRICKS_WORKSPACE_URL>/api/2.0/preview/mlflow/transition-requests/create -d '{"comment": "Please move this model into production!", "model_version": {"version": 1, "registered_model": {"name": "power-forecasting-model"}}, "stage": "Production"}'
# MAGIC ```

# COMMAND ----------

# MAGIC %md Now that you've learned about stage transitions, transition the model to the `Production` stage.

# COMMAND ----------

client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  stage='Production',
)

# COMMAND ----------

# MAGIC %md Use the `MlflowClient.get_model_version_details()` function to fetch the model's current stage.

# COMMAND ----------

model_version_details = client.get_model_version_details(
  name=model_details.name,
  version=model_details.version,
)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

# COMMAND ----------

# MAGIC %md The MLflow Model Registry allows multiple model versions to share the same stage. When referencing a model by stage, the Model Registry will use the latest model version (the model version with the largest version ID). The `MlflowClient.get_latest_versions()` function fetches the latest model version for a given stage or set of stages. The following cell uses this function to print the latest version of the power forecasting model that is in the `Production` stage.

# COMMAND ----------

latest_version_info = client.get_latest_versions(model_name, stages=["Production"])
latest_production_version = latest_version_info[0].version
print("The latest production version of the model '%s' is '%s'." % (model_name, latest_production_version))

# COMMAND ----------

# MAGIC %md # Integrate the model with the forecasting application
# MAGIC 
# MAGIC Now that you have trained and registered a power forecasting model with the MLflow Model Registry, the next step is to integrate it with an application. This application fetches a weather forecast for the wind farm over the next five days and uses the model to produce power forecasts. For example purposes, the application consists of a simple `forecast_power()` function (defined below) that is executed within this notebook. In practice, you may want to execute this function as a recurring batch inference job using the Databricks Jobs service.
# MAGIC 
# MAGIC The following **"Load versions of the registed model"** section demonstrates how to load model versions from the MLflow Model Registry for use in applications. Then, the **"Forecast power output with the production model"** section uses the `Production` model to forecast power output for the next five days.

# COMMAND ----------

# MAGIC %md ## Load versions of the registered model
# MAGIC 
# MAGIC The MLflow Models component defines functions for loading models from several machine learning frameworks. For example, `mlflow.keras.load_model()` is used to load Keras models that were saved in MLflow format, and `mlflow.sklearn.load_model()` is used to load scikit-learn models that were saved in MLflow format.
# MAGIC 
# MAGIC These functions can load models from the MLflow Model Registry.

# COMMAND ----------

# MAGIC %md You can load a model by specifying its name (e.g., `power-forecast-model`) and version number (e.g., `1`). The following cell uses the `mlflow.pyfunc.load_model()` API to load `Version 1` of the registered power forecasting model as a generic Python function.

# COMMAND ----------

model_name = "ravi-power-forecasting-model"

# COMMAND ----------

import mlflow.pyfunc

model_version_uri = "models:/{model_name}/1".format(model_name=model_name)

print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_version_uri))
model_version_1 = mlflow.pyfunc.load_model(model_version_uri)

# COMMAND ----------

# MAGIC %md You can also load a specific model stage. The following cell loads the `Production` stage of the power forecasting model.

# COMMAND ----------

model_production_uri = "models:/{model_name}/production".format(model_name=model_name)

print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_production_uri))
model_production = mlflow.pyfunc.load_model(model_production_uri)

# COMMAND ----------

# MAGIC %md ## Forecast power output with the production model
# MAGIC 
# MAGIC In this section, the production model is used to evaluate weather forecast data for the wind farm. The `forecast_power()` application loads the latest version of the forecasting model from the specified stage and uses it to forecast power production over the next five days.

# COMMAND ----------

def plot(model_name, model_stage, model_version, power_predictions, past_power_output):
  import pandas as pd
  import matplotlib.dates as mdates
  from matplotlib import pyplot as plt
  index = power_predictions.index
  fig = plt.figure(figsize=(11, 7))
  ax = fig.add_subplot(111)
  ax.set_xlabel("Date", size=20, labelpad=20)
  ax.set_ylabel("Power\noutput\n(MW)", size=20, labelpad=60, rotation=0)
  ax.tick_params(axis='both', which='major', labelsize=17)
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
  ax.plot(index[:len(past_power_output)], past_power_output, label="True", color="red", alpha=0.5, linewidth=4)
  ax.plot(index, power_predictions, "--", label="Predicted by '%s'\nin stage '%s' (Version %d)" % (model_name, model_stage, model_version), color="blue", linewidth=3)
  ax.set_ylim(ymin=0, ymax=max(3500, int(max(power_predictions.values) * 1.3)))
  ax.legend(fontsize=14)
  plt.title("Wind farm power output and projections", size=24, pad=20)
  plt.tight_layout()
  display(plt.show())
  
def forecast_power(model_name, model_stage):
  from mlflow.tracking.client import MlflowClient
  client = MlflowClient()
  model_version = client.get_latest_versions(model_name, stages=[model_stage])[0].version
  model_uri = "models:/{model_name}/{model_stage}".format(model_name=model_name, model_stage=model_stage)
  model = mlflow.pyfunc.load_model(model_uri)
  weather_data, past_power_output = get_weather_and_forecast()
  power_predictions = pd.DataFrame(model.predict(weather_data))
  power_predictions.index = pd.to_datetime(weather_data.index)
  print(power_predictions)
  plot(model_name, model_stage, int(model_version), power_predictions, past_power_output)

# COMMAND ----------

forecast_power(model_name, "Production")

# COMMAND ----------

# MAGIC %md # Create and deploy a new model version
# MAGIC 
# MAGIC The MLflow Model Registry enables you to create multiple model versions corresponding to a single registered model. By performing stage transitions, you can seamlessly integrate new model versions into your staging or production environments. Model versions can be trained in different machine learning frameworks (e.g., `scikit-learn` and `Keras`); MLflow's `python_function` provides a consistent inference API across machine learning frameworks, ensuring that the same application code continues to work when a new model version is introduced.
# MAGIC 
# MAGIC The following sections create a new version of the power forecasting model using scikit-learn, perform model testing in `Staging`, and update the production application by transitioning the new model version to `Production`.

# COMMAND ----------

# MAGIC %md ## Create a new model version
# MAGIC 
# MAGIC Classical machine learning techniques are also effective for power forecasting. The following cell trains a random forest model using scikit-learn and registers it with the MLflow Model Registry via the `mlflow.sklearn.log_model()` function.

# COMMAND ----------

import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

with mlflow.start_run():
  n_estimators = 300
  mlflow.log_param("n_estimators", n_estimators)
  
  rand_forest = RandomForestRegressor(n_estimators=n_estimators)
  rand_forest.fit(X_train, y_train)

  val_x, val_y = get_validation_data()
  mse = mean_squared_error(rand_forest.predict(val_x), val_y)
  print("Validation MSE: %d" % mse)
  mlflow.log_metric("mse", mse)
  
  # Specify the `registered_model_name` parameter of the `mlflow.sklearn.log_model()`
  # function to register the model with the MLflow Model Registry. This automatically
  # creates a new model version
  mlflow.sklearn.log_model(
    sk_model=rand_forest,
    artifact_path="sklearn-model",
    registered_model_name=model_name,
  )

# COMMAND ----------

# MAGIC %md ### Fetch the new model version ID using MLflow Model Registry Search
# MAGIC 
# MAGIC The `MlflowClient.search_model_versions()` function searches for model versions by model name, MLflow run ID, or artifact source location. All model versions satisfying a particular filter query are returned.
# MAGIC 
# MAGIC The following cell uses this search function to fetch the version ID of the new model, which is assumed to be the largest (e.g., most recent) version ID.

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
client = MlflowClient()

model_version_infos = client.search_model_versions("name = '%s'" % model_name)
new_model_version = max([model_version_info.version for model_version_info in model_version_infos])

# COMMAND ----------

# MAGIC %md Wait for the new model version to become ready.

# COMMAND ----------

wait_until_ready(model_name, new_model_version)

# COMMAND ----------

# MAGIC %md ## Add a description to the new model version

# COMMAND ----------

client.update_model_version(
  name=model_name,
  version=new_model_version,
  description="This model version is a random forest containing 100 decision trees that was trained in scikit-learn."
)

# COMMAND ----------

# MAGIC %md ## Test the new model version in `Staging`
# MAGIC 
# MAGIC Before deploying a model to a production application, it is often best practice to test it in a staging environment. The following cells transition the new model version to `Staging` and evaluate its performance.

# COMMAND ----------

client.update_model_version(
  name=model_name,
  version=new_model_version,
  stage="Staging",
)

# COMMAND ----------

# MAGIC %md Evaluate the new model's forecasting performance in `Staging`

# COMMAND ----------

forecast_power(model_name, "Staging")

# COMMAND ----------

# MAGIC %md ## Deploy the new model version to `Production`
# MAGIC 
# MAGIC After verifying that the new model version performs well in staging, the following cells transition the model to `Production` and use the exact same application code from the **"Forecast power output with the production model"** section to produce a power forecast.
# MAGIC 
# MAGIC There are now two model versions of the forecasting model in the `Production` stage: the model version trained in Keras model and the version trained in scikit-learn. **When referencing a model by stage, the MLflow Model Model Registry automatically uses the latest production version. This enables you to update your production models without changing any application code**.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/multiple_prod_stage.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="800">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

client.update_model_version(
  name=model_name,
  version=new_model_version,
  stage="Production",
)

# COMMAND ----------

forecast_power(model_name, "Production")

# COMMAND ----------

# MAGIC %md # Archive and delete models
# MAGIC 
# MAGIC When a model version is no longer being used, you can archive it or delete it. You can also delete an entire registered model; this removes all of its associated model versions.

# COMMAND ----------

# MAGIC %md ## Archive `Version 1` of the power forecasting model
# MAGIC 
# MAGIC Archive `Version 1` of the power forecasting model because it is no longer being used. You can archive models in the MLflow Model Registry UI or via the MLflow API.

# COMMAND ----------

# MAGIC %md ### Workflow 1: Archive `Version 1` in the MLflow UI
# MAGIC 
# MAGIC To archive `Version 1` of the power forecasting model, open its corresponding Model Version page in the MLflow Model Registry UI. Then, click the `Stage` button, select `Transition To -> Archived`, and press `OK` in the stage transition confirmation window.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/stage_transition_archived.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="600">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/confirm_archived_transition.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="600">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/stage_archived.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="800">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md ### Workflow 2: Archive `Version 1` using the MLflow API
# MAGIC 
# MAGIC The following cell uses the `MlflowClient.update_model_version()` function to archive `Version 1` of the power forecasting model.

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()
client.update_model_version(
  name=model_name,
  version=1,
  stage="Archived",
)

# COMMAND ----------

# MAGIC %md ## Delete `Version 1` of the power forecasting model
# MAGIC 
# MAGIC You can also use the MLflow UI or MLflow API to delete model versions. **Note that model version deletion is permanent and cannot be undone.**
# MAGIC 
# MAGIC The following cells provide a reference for deleting `Version 1` of the power forecasting model using the Mlflow UI and the MLflow API. 

# COMMAND ----------

# MAGIC %md ### Workflow 1: Delete `Version 1` in the MLflow UI
# MAGIC 
# MAGIC To delete `Version 1` of the power forecasting model, open its corresponding Model Version page in the MLflow Model Registry UI. Then, select the drop-down arrow next to the version identifier and click `Delete`.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/delete_version.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="1000">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md ### Workflow 2: Delete `Version 1` using the MLflow API
# MAGIC 
# MAGIC The following cell permanently deletes `Version 1` of the power forecasting model. If you want to delete this model version, uncomment and execute the cell.

# COMMAND ----------

# client.delete_model_version(
#   name=model_name,
#   version=1,
# )

# COMMAND ----------

# MAGIC %md ## Delete the power forecasting model
# MAGIC 
# MAGIC If you want to delete an entire registered model, including all of its model versions, you can use the `MlflowClient.delete_registered_model()` to do so. This action cannot be undone.
# MAGIC 
# MAGIC **WARNING: The following cell permanently deletes the power forecasting model, including all of its versions.** If you want to delete the model, uncomment and execute the cell. 

# COMMAND ----------

# client.delete_registered_model(name=model_name)

# COMMAND ----------

