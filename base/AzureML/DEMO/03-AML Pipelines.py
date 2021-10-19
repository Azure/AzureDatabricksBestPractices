# Databricks notebook source
# MAGIC %md # Azure Machine Learning Pipeline with AutoMLStep (AutoML training in pipeline)
# MAGIC This notebook demonstrates the use of **AutoMLStep** for training in Azure Machine Learning Pipeline.
# MAGIC As secondary pipeline step, it also uses a **PythonScriptStep** for registering the trained model into AML Workspace model registry.

# COMMAND ----------

# MAGIC %md ## Introduction
# MAGIC In this example we showcase AML Pipelines, how you can train a model with AutoML as one pipeline step with AutoMLStep while using AML Datasets as input data. 
# MAGIC 
# MAGIC If you are using an Azure Machine Learning Compute Instance (aka. Notebook VM), you are all set. Otherwise, make sure you have executed the [configuration](https://aka.ms/pl-config) and [Azure Automated ML setup](https://github.com/Azure/MachineLearningNotebooks/tree/master/how-to-use-azureml/automated-machine-learning#setup-using-a-local-conda-environment) before running this notebook.
# MAGIC 
# MAGIC In this notebook you will learn how to:
# MAGIC 
# MAGIC 1. Create an `Experiment` in an existing `Workspace`.
# MAGIC 2. Create or Attach existing AmlCompute to a workspace.
# MAGIC 3. Define data loading in a **TabularDataset**.
# MAGIC 4. Configure AutoML using **AutoMLConfig**.
# MAGIC 5. Configure **AutoMLStep** step for training
# MAGIC 6. Configure **PythonScriptStep** for registering the model in the Workspace
# MAGIC 7. Run the AML pipeline using AmlCompute
# MAGIC 8. Explore the results.
# MAGIC 9. Test the best fitted model.
# MAGIC 10. Publish the Pipeline in the Workspace

# COMMAND ----------

# MAGIC %md ## Azure Machine Learning and Pipeline SDK-specific imports

# COMMAND ----------

import logging
import os
import csv

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from sklearn import datasets
import pkg_resources

import azureml.core
from azureml.core.experiment import Experiment
from azureml.core.workspace import Workspace
from azureml.train.automl import AutoMLConfig
from azureml.core.compute import AmlCompute
from azureml.core.compute import ComputeTarget, DatabricksCompute
from azureml.core.dataset import Dataset
from azureml.core.runconfig import RunConfiguration
from azureml.core.conda_dependencies import CondaDependencies

from azureml.pipeline.steps import AutoMLStep

# COMMAND ----------

# MAGIC %md ## Check Azure ML SDK version

# COMMAND ----------

import azureml.core
print("You are currently using version", azureml.core.VERSION, "of the Azure ML SDK")

# COMMAND ----------

# MAGIC %md ## Initialize Workspace
# MAGIC Initialize a workspace object from persisted configuration. Make sure the config file is present at .\config.json

# COMMAND ----------

from azureml.core import Workspace
from azureml.core.authentication import InteractiveLoginAuthentication
import os

subscription_id = "5763fde3-4253-480c-928f-dfe1e8888a57"  #you should be owner or contributor
resource_group = "dbx_tech_summit" #you should be owner or contributor
workspace_name = "aml_dbx_summit"
workspace_region = "eastus2"

int_auth = InteractiveLoginAuthentication(tenant_id="72f988bf-86f1-41af-91ab-2d7cd011db47")
ws = Workspace.get(name = workspace_name,
                      subscription_id = subscription_id,
                      resource_group = resource_group,
                      auth=int_auth)

print('Workspace name: ' + ws.name, 
      'Azure region: ' + ws.location, 
      'Subscription id: ' + ws.subscription_id, 
      'Resource group: ' + ws.resource_group, sep = '\n')


# COMMAND ----------

from azureml.core import Environment

envs = Environment.list(workspace=ws)

# List Environments and packages in my workspace
for env in envs:
    if env.startswith("AzureML"):
        print("Name",env)
        print("packages", envs[env].python.conda_dependencies.serialize_to_string())
        
        if env.startswith("AzureML-AutoML"):
            print("Packages in AzureML-AutoML curated Environment")
            print("packages", envs[env].python.conda_dependencies.serialize_to_string())

# COMMAND ----------

# MAGIC %md ## Create an Azure ML experiment
# MAGIC Let's create an experiment named "automlstep-classif-porto".

# COMMAND ----------

# Choose a name for the run history container in the workspace.
experiment_name = 'automlstep-classif-porto'
experiment = Experiment(ws, experiment_name)
experiment

# COMMAND ----------

# MAGIC %md ### Create or Attach an AmlCompute cluster
# MAGIC You will need to create a [compute target](https://docs.microsoft.com/azure/machine-learning/service/concept-azure-machine-learning-architecture#compute-target) for your AutoML run. In this tutorial, you get the default `AmlCompute` as your training compute resource.

# COMMAND ----------

db_compute_name = "dbxcluster"
try:
    databricks_compute = DatabricksCompute(workspace=ws, name=db_compute_name)
    print('Compute target {} already exists'.format(db_compute_name))
except ComputeTargetException:
    print('Compute not found, will use below parameters to attach new one')
    print('db_compute_name {}'.format(db_compute_name))
    print('db_resource_group {}'.format(db_resource_group))
    print('db_workspace_name {}'.format(db_workspace_name))
    print('db_access_token {}'.format(db_access_token))
 
    config = DatabricksCompute.attach_configuration(
        resource_group = db_resource_group,
        workspace_name = db_workspace_name,
        access_token= db_access_token)
    databricks_compute=ComputeTarget.attach(ws, db_compute_name, config)
    databricks_compute.wait_for_completion(True)

# COMMAND ----------

from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException

# Choose a name for your CPU cluster
dbx_cluster_name = "dbxcluster"

dbx_cluster = ComputeTarget(workspace=ws, name=dbx_cluster_name)


# COMMAND ----------

from azureml.core.compute import AmlCompute, ComputeTarget
# Define remote compute target to use
# Further docs on Remote Compute Target: https://docs.microsoft.com/en-us/azure/machine-learning/how-to-auto-train-remote

# Choose a name for your cluster.
amlcompute_cluster_name = "cpucluster"


found = False
# Check if this compute target already exists in the workspace.
cts = ws.compute_targets

if amlcompute_cluster_name in cts and cts[amlcompute_cluster_name].type == 'AmlCompute':
     found = True
     print('Found existing training cluster.')
     # Get existing cluster
     # Method 1:
     aml_remote_compute = cts[amlcompute_cluster_name]
     # Method 2:
     # aml_remote_compute = ComputeTarget(ws, amlcompute_cluster_name)
    
if not found:
     print('Creating a new training cluster...')
     provisioning_config = AmlCompute.provisioning_configuration(vm_size = "STANDARD_D13_V2", # for GPU, use "STANDARD_NC12"
                                                                 #vm_priority = 'lowpriority', # optional
                                                                 max_nodes = 5)
     # Create the cluster.
     aml_remote_compute = ComputeTarget.create(ws, amlcompute_cluster_name, provisioning_config)
    
print('Checking cluster status...')
# Can poll for a minimum number of nodes and for a specific timeout.
# If no min_node_count is provided, it will use the scale settings for the cluster.
aml_remote_compute.wait_for_completion(show_output = True, min_node_count = 0, timeout_in_minutes = 20)

# For a more detailed view of current AmlCompute status, use get_status().

# COMMAND ----------

# MAGIC %md ## Data

# COMMAND ----------

# MAGIC %md ### (***Optional***) Submit dataset file into DataStore (Azure Blob under the covers)

# COMMAND ----------

datastore = ws.get_default_datastore()
datastore.upload(src_dir='../../data/', 
                 target_path='Datasets/porto_seguro_safe_driver_prediction', overwrite=True, show_progress=True)

# COMMAND ----------

# MAGIC %md ## Load data into Azure ML Dataset and Register into Workspace

# COMMAND ----------

# Try to load the dataset from the Workspace. Otherwise, create it from the file in the HTTP URL
found = False
aml_dataset_name = "safedriverdataset"

if aml_dataset_name in ws.datasets.keys(): 
       found = True
       dataset = ws.datasets[aml_dataset_name] 
       print("Dataset found and loaded from the Workspace")
       
if not found:
        # Create AML Dataset and register it into Workspace
        print("Dataset does not exist in the current Workspace. It will be imported and registered.")
        
        # Option A: Create AML Dataset from file in AML DataStore
        datastore = ws.get_default_datastore()
        dataset = Dataset.Tabular.from_delimited_files(path=datastore.path('Datasets/porto_seguro_safe_driver_prediction/porto_seguro_safe_driver_prediction_train.csv'))
        data_origin_type = 'AMLDataStore'
        
        # Option B: Create AML Dataset from file in HTTP URL
        # data_url = 'https://azmlworkshopdata.blob.core.windows.net/safedriverdata/porto_seguro_safe_driver_prediction_train.csv'
        # aml_dataset = Dataset.Tabular.from_delimited_files(data_url)  
        # data_origin_type = 'HttpUrl'
        
        print(aml_dataset)
                
        #Register Dataset in Workspace
        registration_method = 'SDK'  # or 'UI'
        dataset = aml_dataset.register(workspace=ws,
                                           name=aml_dataset_name,
                                           description='Porto Seguro Safe Driver Prediction Train dataset file',
                                           tags={'Registration-Method': registration_method, 'Data-Origin-Type': data_origin_type},
                                           create_new_version=True)
        
        print("Dataset created from file and registered in the Workspace")

# COMMAND ----------

dataset.take(5).to_pandas_dataframe().head(3)

# COMMAND ----------

# MAGIC %md ### Segregate a Test dataset for later testing and creating a confusion matrix
# MAGIC Split original AML Tabular Dataset in two test/train AML Tabular Datasets (using AML DS function)

# COMMAND ----------

# The name and target column of the Dataset to create 
train_dataset_name = "porto_seguro_safe_driver_prediction_train90"

# COMMAND ----------

# Split using Azure Tabular Datasets (Better for Remote Compute)
# https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.data.tabulardataset?view=azure-ml-py#random-split-percentage--seed-none-

train_dataset, test_dataset = dataset.random_split(0.9, seed=1)



# COMMAND ----------

#Register Train Dataset (90%) after Split in Workspace
registration_method = 'SDK'  # or 'UI'
data_origin_type = 'SPLIT'
train_dataset = train_dataset.register(workspace=ws,
                                       name=train_dataset_name,
                                       description='Porto Seguro Safe Driver Prediction Train dataset file (90%)',
                                       tags={'Registration-Method': registration_method, 'Data-Origin-Type': data_origin_type},
                                       create_new_version=True)

# COMMAND ----------

# Load from Workspace
# The name and target column of the Dataset to create 
train_dataset_name = "porto_seguro_safe_driver_prediction_train90"

train_dataset = ws.datasets[train_dataset_name] 
train_dataset

# COMMAND ----------

# Use Pandas DF only to check the data
train_df = train_dataset.to_pandas_dataframe()
test_df = test_dataset.to_pandas_dataframe()

print(train_df.shape)
print(test_df.shape)

train_df.describe()

# COMMAND ----------

# MAGIC %md ### List possible metrics to optimize for (primary metric) in Classification using AutoML

# COMMAND ----------

from azureml.train import automl

# List of possible primary metrics is here:
# https://docs.microsoft.com/en-us/azure/machine-learning/how-to-configure-auto-train#primary-metric
    
# Get a list of valid metrics for your given task
automl.utilities.get_primary_metrics('classification')

# COMMAND ----------

# MAGIC %md ## Train configuration in AutoMLConfig
# MAGIC This creates a general AutoML settings object.
# MAGIC 
# MAGIC Note that we're using a single algorithm (LightGBM as the only whitelisted algo) and a single iteration (iterations=1) just for the sake of time when trying the notebook so the end-to-end run needs less time.
# MAGIC 
# MAGIC In a real training run you would let AutoML to try many iterations in order to find the "best model".

# COMMAND ----------

import logging
from azureml.train.automl import AutoMLConfig

# Path to the Azure ML project folder
project_folder = './project'

# You can provide additional settings as a **kwargs parameter for the AutoMLConfig object
automl_settings = {
      "blacklist_models":['LogisticRegression', 'ExtremeRandomTrees', 'RandomForest'], 
      # "whitelist_models": ['LightGBM'],
      # "n_cross_validations": 5,
      # "validation_data": test_dataset,   # Better to holdout the Test Dataset
      "experiment_exit_score": 0.7
}

automl_config = AutoMLConfig(compute_target=aml_remote_compute,
                             task='classification',
                             primary_metric='AUC_weighted',                           
                             training_data=train_dataset, # AML Dataset
                             label_column_name="target",
                             path = project_folder,
                             enable_early_stopping= True,
                             iterations=5,
                             max_concurrent_iterations=5,
                             experiment_timeout_hours=1,                           
                             featurization= 'auto',   # (auto/off) All feature columns in this dataset are numbers, no need to featurize with AML Dataset. 
                             debug_log='automated_ml_errors.log',
                             verbosity= logging.INFO,
                             model_explainability=True,
                             enable_onnx_compatible_models=False,
                             spark_context=sc, 
                             **automl_settings
                             )

# Explanation of Settings: https://docs.microsoft.com/en-us/azure/machine-learning/how-to-configure-auto-train#configure-your-experiment-settings

# AutoMLConfig info on: 
# https://docs.microsoft.com/en-us/python/api/azureml-train-automl-client/azureml.train.automl.automlconfig.automlconfig

# COMMAND ----------

# MAGIC %md # The AML Pipeline
# MAGIC 
# MAGIC ### PipelineData objects
# MAGIC 
# MAGIC Let's first define the data (metrics and model) to be produced by the pipeline.
# MAGIC 
# MAGIC The **PipelineData** object is a special kind of data reference that is used for interim storage locations that can be passed between pipeline steps.
# MAGIC 
# MAGIC Note that we also need to pass it as a script argument so our code can access the datastore location referenced by the data reference.

# COMMAND ----------

from azureml.pipeline.core import PipelineData, TrainingOutput

ds = ws.get_default_datastore()
metrics_output_name = 'metrics_output'
best_model_output_name = 'best_model_output'

metrics_data = PipelineData(name='metrics_data',
                            datastore=ds,
                            pipeline_output_name=metrics_output_name,
                            training_output=TrainingOutput(type='Metrics'))

model_data = PipelineData(name='model_data',
                          datastore=ds,
                          pipeline_output_name=best_model_output_name,
                          training_output=TrainingOutput(type='Model'))

print(model_data.get_env_variable_name())

# COMMAND ----------

# MAGIC %md ## Create an AutoMLStep for training.
# MAGIC Pipelines consist of one or more *steps*, which can be specialized steps like an AutoMLStep (like this step) for training a model, or Python scripts, or a data transfer step that copies data from one location to another. 
# MAGIC 
# MAGIC Each step can run in its own compute context.

# COMMAND ----------

automl_step = AutoMLStep(
    name='automl_module',
    automl_config=automl_config,
    outputs=[metrics_data, model_data],
    allow_reuse=False)

# COMMAND ----------

# MAGIC %md ## Create a PythonScriptStep to register the model in the Workspace.

# COMMAND ----------

# MAGIC %md Create Environment and Conda dependencies needed to run the PythonScriptStep.

# COMMAND ----------

# MAGIC %md ### Environment, Conda-Dependencies and RunConfiguration for PythonScriptStep

# COMMAND ----------

from azureml.core.runconfig import CondaDependencies, DEFAULT_CPU_IMAGE, RunConfiguration
from azureml.core import Environment

# Create an Environment for future usage
custom_env = Environment("python-script-step-env")
custom_env.python.user_managed_dependencies = False # Let Azure ML manage dependencies
custom_env.docker.enabled = True 
custom_env.docker.base_image = azureml.core.runconfig.DEFAULT_CPU_IMAGE 

conda_dependencies = CondaDependencies.create(pip_packages=['azureml-sdk[automl]', 'applicationinsights'], #'azureml-explain-model'
                                              conda_packages=['numpy==1.16.2'], 
                                              pin_sdk_version=False)

# Add the dependencies to the environment
custom_env.python.conda_dependencies = conda_dependencies

# Register the environment (To use it again)
custom_env.register(workspace=ws)
registered_env = Environment.get(ws, 'python-script-step-env')

# create a new RunConfig object
conda_run_config = RunConfiguration(framework="python")

# Set compute target to AmlCompute
conda_run_config.target = aml_remote_compute

# Assign the environment to the run configuration
conda_run_config.environment
conda_run_config.environment = registered_env

print('Run config is ready')

# COMMAND ----------

# MAGIC %md ## Register Model Step (Python Script Step)
# MAGIC Script to register the model to the workspace.
# MAGIC 
# MAGIC First, let's create a folder where the script will be placed.
# MAGIC 
# MAGIC The best practice is to use separate folders for scripts and its dependent files for each step and specify that folder as the `source_directory` for the step. This helps reduce the size of the snapshot created for the step (only the specific folder is snapshotted). Since changes in any files in the `source_directory` would trigger a re-upload of the snapshot, this helps keep the reuse of the step when there are no changes in the `source_directory` of the step.

# COMMAND ----------

import os
import shutil
scripts_folder="Scripts"
os.makedirs(scripts_folder, exist_ok=True)

# COMMAND ----------

# MAGIC %md ### Create the register_model.py script file

# COMMAND ----------

# MAGIC %%writefile $scripts_folder/register_model.py
# MAGIC from azureml.core.model import Model, Dataset
# MAGIC from azureml.core.run import Run, _OfflineRun
# MAGIC from azureml.core import Workspace
# MAGIC from azureml.core.resource_configuration import ResourceConfiguration
# MAGIC import argparse
# MAGIC 
# MAGIC parser = argparse.ArgumentParser()
# MAGIC parser.add_argument("--model_name")
# MAGIC parser.add_argument("--model_path")
# MAGIC parser.add_argument("--ds_name")
# MAGIC args = parser.parse_args()
# MAGIC 
# MAGIC print("Argument 1(model_name): %s" % args.model_name)
# MAGIC print("Argument 2(model_path): %s" % args.model_path)
# MAGIC print("Argument 3(ds_name): %s" % args.ds_name)
# MAGIC 
# MAGIC run = Run.get_context()
# MAGIC ws = None
# MAGIC if type(run) == _OfflineRun:
# MAGIC     ws = Workspace.from_config()
# MAGIC else:
# MAGIC     ws = run.experiment.workspace
# MAGIC 
# MAGIC train_ds = Dataset.get_by_name(ws, args.ds_name)
# MAGIC datasets = [(Dataset.Scenario.TRAINING, train_ds)]
# MAGIC 
# MAGIC # Register model with training dataset
# MAGIC 
# MAGIC model = Model.register(workspace=ws,
# MAGIC                        model_path=args.model_path,                  # File to upload and register as a model.
# MAGIC                        model_name=args.model_name,                  # Name of the registered model in your workspace.
# MAGIC                        datasets=datasets,
# MAGIC                        description='Porto Seguro Safe Driving Prediction.',
# MAGIC                        tags={'area': 'insurance', 'type': 'classification'}
# MAGIC                       )
# MAGIC 
# MAGIC print("Registered version {0} of model {1}".format(model.version, model.name))

# COMMAND ----------

# MAGIC %md ### PythonScriptStep to run register_model.py script

# COMMAND ----------

from azureml.pipeline.core import PipelineParameter

# The model name with which to register the trained model in the workspace.
model_name = "porto-model-from-automlstep"
model_name_param = PipelineParameter("model_name", default_value=model_name)

# The Dataset name to relate with the model to register in the workspace.
dataset_name_param = PipelineParameter(name="ds_name", default_value=train_dataset_name)

# COMMAND ----------

# Create PythonScriptStep for Model registration

from azureml.pipeline.core import PipelineData
from azureml.pipeline.steps import PythonScriptStep

register_model_step = PythonScriptStep(name="register_model",                                      
                                       source_directory = scripts_folder,            # Local folder with .py script
                                       script_name="register_model.py",
                                       allow_reuse=False,
                                       arguments=["--model_name", model_name_param, "--model_path", model_data, "--ds_name", dataset_name_param],
                                       inputs=[model_data],
                                       compute_target=aml_remote_compute,
                                       runconfig=conda_run_config)

register_model_step.run_after(automl_step)

print("Pipeline steps defined")

# COMMAND ----------

# MAGIC %md This is a simple example, designed to demonstrate the principle. In reality, you could build more sophisticated logic into the pipeline steps - for example, evaluating the model against some test data to calculate a performance metric like AUC or accuracy, comparing the metric to that of any previously registered versions of the model, and only registering the new model if it performs better.

# COMMAND ----------

# MAGIC %md ## Create Pipeline and add the multiple steps into it

# COMMAND ----------

from azureml.pipeline.core import Pipeline
pipeline = Pipeline(
    description="pipeline_with_automlstep",
    workspace=ws,    
    steps=[automl_step, register_model_step]) 
#
print("Pipeline is built.")

# COMMAND ----------

pipeline_run = experiment.submit(pipeline, pipeline_parameters={
        "ds_name": train_dataset_name, "model_name": model_name})

print("Pipeline submitted for execution.")

# COMMAND ----------

pipeline_run.wait_for_completion()

# COMMAND ----------

# MAGIC %md ## Examine Results from Pipeline
# MAGIC 
# MAGIC ### Retrieve the metrics of all child runs
# MAGIC Outputs of above run can be used as inputs of other steps in pipeline. In this tutorial, we will examine the outputs by retrieve output data and running some tests.

# COMMAND ----------

# MAGIC %md
# MAGIC ### run this if you are pulling an existing pipeline run

# COMMAND ----------

from azureml.pipeline.core.run import PipelineRun


run_id="a6205940-69fb-4c60-ba84-3d18a6217ed6"
metrics_output_name = 'metrics_output'
best_model_output_name = 'best_model_output'
pipeline_run = PipelineRun(ws.experiments[experiment_name], run_id)


# COMMAND ----------


metrics_output = pipeline_run.get_pipeline_output(metrics_output_name)
num_file_downloaded = metrics_output.download('.', show_progress=True)

# COMMAND ----------

import json
with open(metrics_output._path_on_datastore) as f:  
   metrics_output_result = f.read()
    
deserialized_metrics_output = json.loads(metrics_output_result)
df = pd.DataFrame(deserialized_metrics_output)
df

# COMMAND ----------

# MAGIC %md ### Retrieve info about the trained model

# COMMAND ----------

print(pipeline_run.get_file_names())

# COMMAND ----------

best_model_output = pipeline_run.get_pipeline_output(best_model_output_name)
best_model_output
# num_file_downloaded = best_model_output.download('.', show_progress=True)

# COMMAND ----------

# MAGIC %md ### Retrieve the Best Model

# COMMAND ----------

best_model_output = pipeline_run.get_pipeline_output(best_model_output_name)
num_file_downloaded = best_model_output.download('.', show_progress=True)

# COMMAND ----------

import pickle

with open(best_model_output._path_on_datastore, "rb" ) as f:
    best_model = pickle.load(f)
best_model

# COMMAND ----------

# MAGIC %md ### Test the Model (Make predictions, get probabilities and calculate metrics)

# COMMAND ----------

# MAGIC %md #### Prep Test Data: Extract X values (feature columns) from test dataset and convert to NumPi array for predicting 

# COMMAND ----------

import pandas as pd

x_test_df = test_df.copy()

if 'target' in x_test_df.columns:
    y_test_df = x_test_df.pop('target')

print(test_df.shape)
print(x_test_df.shape)
print(y_test_df.shape)

# COMMAND ----------

# MAGIC %md # Testing Our Best Fitted Model

# COMMAND ----------

# Try the best model making predictions with the test dataset
y_predictions = best_model.predict(x_test_df)

print('10 predictions: ')
print(y_predictions[:10])

# COMMAND ----------

# MAGIC %md ### Calculate Accuracy

# COMMAND ----------

from sklearn.metrics import accuracy_score

print('Accuracy with Scikit-Learn model:')
print(accuracy_score(y_test_df, y_predictions))


# COMMAND ----------

# MAGIC %md ### Calculate AUC with Test Dataset

# COMMAND ----------

# Get Probabilities/scores per class

class_probabilities = best_model.predict_proba(x_test_df)
print(class_probabilities.shape)

print('Some class probabilities...: ')
print(class_probabilities[:3])

# COMMAND ----------

from sklearn import metrics
from sklearn.metrics import roc_auc_score

print('ROC AUC:')
print(roc_auc_score(y_test_df, class_probabilities[:,1]))

print('ROC AUC Weighted:')
print(roc_auc_score(y_test_df, class_probabilities[:,1], average='weighted'))

# COMMAND ----------

# MAGIC %md ## Show Confusion Matrix
# MAGIC We will use confusion matrix to see how our model works.

# COMMAND ----------

# Need to have installed: 
!pip install pandas_ml

# COMMAND ----------

from pandas_ml import ConfusionMatrix

cm = ConfusionMatrix(y_test_df, y_predictions)

print(cm)

cm.plot()

# COMMAND ----------

# MAGIC %md ## Publish the Pipeline
# MAGIC Now that you've created a pipeline and verified it works, you can publish it as a REST service.

# COMMAND ----------

published_pipeline = pipeline.publish(name="Training_Pipeline_AutoMLStep_Porto",
                                      description="Pipeline with AutoMLStep for Training model for Porto Seguro Safe Driver")
rest_endpoint = published_pipeline.endpoint
print(rest_endpoint)

# COMMAND ----------

# MAGIC %md # Trigger the AML Pipeline by using the Pipeline REST Endpoint

# COMMAND ----------

# MAGIC %md To use the endpoint, client applications need to make a REST call over HTTP. This request must be authenticated, so an authorization header is required. A real application would require a service principal with which to be authenticated, but to test this out, we'll use the authorization header from your current connection to your Azure workspace, which you can get using the following code:

# COMMAND ----------

from azureml.core.authentication import InteractiveLoginAuthentication

interactive_auth = InteractiveLoginAuthentication()
auth_header = interactive_auth.get_authentication_header()
print(auth_header)

# COMMAND ----------

# MAGIC %md Now you're ready to call the REST interface. The pipeline runs asynchronously, so you'll get an identifier back, which you can use to track the pipeline experiment as it runs:

# COMMAND ----------

# MAGIC %md ### The REST Endpoint
# MAGIC Note that the published pipeline has an endpoint, which you can see in the Endpoints page (on the Pipeline Endpoints tab) in Azure Machine Learning studio. You can also find its URI as a property of the published pipeline object.
# MAGIC So, you could also copy that REST Endpoint from the AML portal and paste it like here:
# MAGIC 
# MAGIC rest_endpoint = "Your opied REST Endpoint here"
# MAGIC     

# COMMAND ----------

import requests

response = requests.post(rest_endpoint, 
                         headers=auth_header, 
                         json={"ExperimentName": experiment_name})
run_id = response.json()["Id"]
run_id

# COMMAND ----------

# MAGIC %md 
# MAGIC ###View in AML Portal the running Pipeline triggered via REST call

# COMMAND ----------

# MAGIC %md # Next Steps!
# MAGIC You can use the Azure Machine Learning extension for Azure DevOps to combine Azure ML pipelines with Azure DevOps pipelines (yes, it is confusing that they have the same name!) and integrate model retraining into a continuous integration/continuous deployment (CI/CD) process. For example you could use an Azure DevOps build pipeline to trigger an Azure ML pipeline that trains and registers a model, and when the model is registered it could trigger an Azure Devops release pipeline that deploys the model as a web service, along with the application or service that consumes the model.