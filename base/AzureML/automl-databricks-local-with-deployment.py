# Databricks notebook source
# MAGIC %md Copyright (c) Microsoft Corporation. All rights reserved.
# MAGIC 
# MAGIC Licensed under the MIT License.

# COMMAND ----------

# MAGIC %md We support installing AML SDK as library from GUI. When attaching a library follow this https://docs.databricks.com/user-guide/libraries.html and add the below string as your PyPi package. You can select the option to attach the library to all clusters or just one cluster.
# MAGIC 
# MAGIC **install azureml-sdk with Automated ML**
# MAGIC * Source: Upload Python Egg or PyPi
# MAGIC * PyPi Name: `azureml-sdk[automl]`
# MAGIC * Select Install Library

# COMMAND ----------

# MAGIC %md # AutoML : Classification with Local Compute on Azure DataBricks with deployment to ACI
# MAGIC 
# MAGIC In this example we use the scikit-learn's [digit dataset](http://scikit-learn.org/stable/datasets/index.html#optical-recognition-of-handwritten-digits-dataset) to showcase how you can use AutoML for a simple classification problem.
# MAGIC 
# MAGIC In this notebook you will learn how to:
# MAGIC 1. Create Azure Machine Learning Workspace object and initialize your notebook directory to easily reload this object from a configuration file.
# MAGIC 2. Create an `Experiment` in an existing `Workspace`.
# MAGIC 3. Configure AutoML using `AutoMLConfig`.
# MAGIC 4. Train the model using AzureDataBricks.
# MAGIC 5. Explore the results.
# MAGIC 6. Register the model.
# MAGIC 7. Deploy the model.
# MAGIC 8. Test the best fitted model.
# MAGIC 
# MAGIC Prerequisites:
# MAGIC Before running this notebook, please follow the readme for installing necessary libraries to your cluster.

# COMMAND ----------

# MAGIC %md ## Register Machine Learning Services Resource Provider
# MAGIC Microsoft.MachineLearningServices only needs to be registed once in the subscription. To register it:
# MAGIC Start the Azure portal.
# MAGIC Select your All services and then Subscription.
# MAGIC Select the subscription that you want to use.
# MAGIC Click on Resource providers
# MAGIC Click the Register link next to Microsoft.MachineLearningServices

# COMMAND ----------

# MAGIC %md ### Check the Azure ML Core SDK Version to Validate Your Installation

# COMMAND ----------

import azureml.core

print("SDK Version:", azureml.core.VERSION)

# COMMAND ----------

# MAGIC %md ## Initialize an Azure ML Workspace
# MAGIC ### What is an Azure ML Workspace and Why Do I Need One?
# MAGIC 
# MAGIC An Azure ML workspace is an Azure resource that organizes and coordinates the actions of many other Azure resources to assist in executing and sharing machine learning workflows.  In particular, an Azure ML workspace coordinates storage, databases, and compute resources providing added functionality for machine learning experimentation, operationalization, and the monitoring of operationalized models.
# MAGIC 
# MAGIC 
# MAGIC ### What do I Need?
# MAGIC 
# MAGIC To create or access an Azure ML workspace, you will need to import the Azure ML library and specify following information:
# MAGIC * A name for your workspace. You can choose one.
# MAGIC * Your subscription id. Use the `id` value from the `az account show` command output above.
# MAGIC * The resource group name. The resource group organizes Azure resources and provides a default region for the resources in the group. The resource group will be created if it doesn't exist. Resource groups can be created and viewed in the [Azure portal](https://portal.azure.com)
# MAGIC * Supported regions include `eastus2`, `eastus`,`westcentralus`, `southeastasia`, `westeurope`, `australiaeast`, `westus2`, `southcentralus`.

# COMMAND ----------

workspace_name = "joel-aml"
workspace_region = "eastus2"
resource_group = "joel-simple"
subscription_id = "3f2e4d32-8e8d-46d6-82bc-5bb8d962328b"

# COMMAND ----------

# MAGIC %md ## Creating a Workspace
# MAGIC If you already have access to an Azure ML workspace you want to use, you can skip this cell.  Otherwise, this cell will create an Azure ML workspace for you in the specified subscription, provided you have the correct permissions for the given `subscription_id`.
# MAGIC 
# MAGIC This will fail when:
# MAGIC 1. The workspace already exists.
# MAGIC 2. You do not have permission to create a workspace in the resource group.
# MAGIC 3. You are not a subscription owner or contributor and no Azure ML workspaces have ever been created in this subscription.
# MAGIC 
# MAGIC If workspace creation fails for any reason other than already existing, please work with your IT administrator to provide you with the appropriate permissions or to provision the required resources.
# MAGIC 
# MAGIC **Note:** Creation of a new workspace can take several minutes.

# COMMAND ----------

# Import the Workspace class and check the Azure ML SDK version.
from azureml.core import Workspace

ws = Workspace.create(name = workspace_name,
                      subscription_id = subscription_id,
                      resource_group = resource_group, 
                      location = workspace_region,                      
                      exist_ok=True)
ws.get_details()

# COMMAND ----------

# MAGIC %md ## Configuring Your Local Environment
# MAGIC You can validate that you have access to the specified workspace and write a configuration file to the default configuration location, `./aml_config/config.json`.

# COMMAND ----------

from azureml.core import Workspace

ws = Workspace(workspace_name = workspace_name,
               subscription_id = subscription_id,
               resource_group = resource_group)

# Persist the subscription id, resource group name, and workspace name in aml_config/config.json.
ws.write_config()
# write_config(path="/databricks/driver/aml_config/",file_name=<alias_conf.cfg>)

# COMMAND ----------

# MAGIC %md ## Create a Folder to Host Sample Projects
# MAGIC Finally, create a folder where all the sample projects will be hosted.

# COMMAND ----------

import os

sample_projects_folder = './sample_projects'

if not os.path.isdir(sample_projects_folder):
    os.mkdir(sample_projects_folder)
    
print('Sample projects will be created in {}.'.format(sample_projects_folder))

# COMMAND ----------

# MAGIC %md ## Create an Experiment
# MAGIC 
# MAGIC As part of the setup you have already created an Azure ML `Workspace` object. For AutoML you will need to create an `Experiment` object, which is a named object in a `Workspace` used to run experiments.

# COMMAND ----------

import logging
import os
import random
import time
import json

from matplotlib import pyplot as plt
from matplotlib.pyplot import imshow
import numpy as np
# import pandas as pd

import azureml.core
from azureml.core.experiment import Experiment
from azureml.core.workspace import Workspace
from azureml.train.automl import AutoMLConfig
from azureml.train.automl.run import AutoMLRun

# COMMAND ----------

# Choose a name for the experiment and specify the project folder.
experiment_name = 'automl-local-classification'
project_folder = './sample_projects/automl-local-classification'

experiment = Experiment(ws, experiment_name)

output = {}
output['SDK version'] = azureml.core.VERSION
output['Subscription ID'] = ws.subscription_id
output['Workspace Name'] = ws.name
output['Resource Group'] = ws.resource_group
output['Location'] = ws.location
output['Project Directory'] = project_folder
output['Experiment Name'] = experiment.name
# pd.set_option('display.max_colwidth', -1)
# pd.DataFrame(data = output, index = ['']).T

# COMMAND ----------

# MAGIC %md ## Registering Datastore

# COMMAND ----------

# MAGIC %md Datastore is the way to save connection information to a storage service (e.g. Azure Blob, Azure Data Lake, Azure SQL) information to your workspace so you can access them without exposing credentials in your code. The first thing you will need to do is register a datastore, you can refer to our [python SDK documentation](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.datastore.datastore?view=azure-ml-py) on how to register datastores. __Note: for best security practices, please do not check in code that contains registering datastores with secrets into your source control__
# MAGIC 
# MAGIC The code below registers a datastore pointing to a publicly readable blob container.

# COMMAND ----------

from azureml.core import Datastore

datastore_name = 'demo_training'
container_name = 'digits' 
account_name = 'automlpublicdatasets'
Datastore.register_azure_blob_container(
    workspace = ws, 
    datastore_name = datastore_name, 
    container_name = container_name, 
    account_name = account_name,
     overwrite = True
)

# COMMAND ----------

# MAGIC %md Below is an example on how to register a private blob container
# MAGIC ```python
# MAGIC datastore = Datastore.register_azure_blob_container(
# MAGIC     workspace = ws, 
# MAGIC     datastore_name = 'example_datastore', 
# MAGIC     container_name = 'example-container', 
# MAGIC     account_name = 'storageaccount',
# MAGIC     account_key = 'accountkey'
# MAGIC )
# MAGIC ```
# MAGIC The example below shows how  to register an Azure Data Lake store. Please make sure you have granted the necessary permissions for the service principal to access the data lake.
# MAGIC ```python
# MAGIC datastore = Datastore.register_azure_data_lake(
# MAGIC     workspace = ws,
# MAGIC     datastore_name = 'example_datastore',
# MAGIC     store_name = 'adlsstore',
# MAGIC     tenant_id = 'tenant-id-of-service-principal',
# MAGIC     client_id = 'client-id-of-service-principal',
# MAGIC     client_secret = 'client-secret-of-service-principal'
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Load Training Data Using Dataset

# COMMAND ----------

# MAGIC %md Automated ML takes a `TabularDataset` as input.
# MAGIC 
# MAGIC You are free to use the data preparation libraries/tools of your choice to do the require preparation and once you are done, you can write it to a datastore and create a TabularDataset from it.
# MAGIC 
# MAGIC You will get the datastore you registered previously and pass it to Dataset for reading. The data comes from the digits dataset: `sklearn.datasets.load_digits()`. `DataPath` points to a specific location within a datastore. 

# COMMAND ----------

from azureml.core.dataset import Dataset
from azureml.data.datapath import DataPath

datastore = Datastore.get(workspace = ws, datastore_name = datastore_name)

X_train = Dataset.Tabular.from_delimited_files(datastore.path('X.csv'))
y_train = Dataset.Tabular.from_delimited_files(datastore.path('y.csv'))

# COMMAND ----------

# MAGIC %md ## Review the TabularDataset
# MAGIC You can peek the result of a TabularDataset at any range using `skip(i)` and `take(j).to_pandas_dataframe()`. Doing so evaluates only j records for all the steps in the TabularDataset, which makes it fast even against large datasets.

# COMMAND ----------

X_train.take(5).to_pandas_dataframe()

# COMMAND ----------

y_train.take(5).to_pandas_dataframe()

# COMMAND ----------

# MAGIC %md ## Configure AutoML
# MAGIC 
# MAGIC Instantiate an `AutoMLConfig` object to specify the settings and data used to run the experiment.
# MAGIC 
# MAGIC |Property|Description|
# MAGIC |-|-|
# MAGIC |**task**|classification or regression|
# MAGIC |**primary_metric**|This is the metric that you want to optimize. Classification supports the following primary metrics: <br><i>accuracy</i><br><i>AUC_weighted</i><br><i>average_precision_score_weighted</i><br><i>norm_macro_recall</i><br><i>precision_score_weighted</i>|
# MAGIC |**primary_metric**|This is the metric that you want to optimize. Regression supports the following primary metrics: <br><i>spearman_correlation</i><br><i>normalized_root_mean_squared_error</i><br><i>r2_score</i><br><i>normalized_mean_absolute_error</i>|
# MAGIC |**iteration_timeout_minutes**|Time limit in minutes for each iteration.|
# MAGIC |**iterations**|Number of iterations. In each iteration AutoML trains a specific pipeline with the data.|
# MAGIC |**n_cross_validations**|Number of cross validation splits.|
# MAGIC |**spark_context**|Spark Context object. for Databricks, use spark_context=sc|
# MAGIC |**max_concurrent_iterations**|Maximum number of iterations to execute in parallel. This should be <= number of worker nodes in your Azure Databricks cluster.|
# MAGIC |**X**|(sparse) array-like, shape = [n_samples, n_features]|
# MAGIC |**y**|(sparse) array-like, shape = [n_samples, ], [n_samples, n_classes]<br>Multi-class targets. An indicator matrix turns on multilabel classification. This should be an array of integers.|
# MAGIC |**path**|Relative path to the project folder. AutoML stores configuration files for the experiment under this folder. You can specify a new empty folder.|
# MAGIC |**preprocess**|set this to True to enable pre-processing of data eg. string to numeric using one-hot encoding|
# MAGIC |**exit_score**|Target score for experiment. It is associated with the metric. eg. exit_score=0.995 will exit experiment after that|

# COMMAND ----------

automl_config = AutoMLConfig(task = 'classification',
                             debug_log = 'automl_errors.log',
                             primary_metric = 'AUC_weighted',
                             iteration_timeout_minutes = 10,
                             iterations = 5,
                             preprocess = True,
                             n_cross_validations = 10,
                             max_concurrent_iterations = 2, #change it based on number of worker nodes
                             verbosity = logging.INFO,
                             spark_context=sc, #databricks/spark related
                             X = X_train, 
                             y = y_train,
                             path = project_folder)

# COMMAND ----------

# MAGIC %md ## Train the Models
# MAGIC 
# MAGIC Call the `submit` method on the experiment object and pass the run configuration. Execution of local runs is synchronous. Depending on the data and the number of iterations this can run for a while.

# COMMAND ----------

local_run = experiment.submit(automl_config, show_output = True)

# COMMAND ----------

# MAGIC %md ## Explore the Results

# COMMAND ----------

# MAGIC %md #### Portal URL for Monitoring Runs
# MAGIC 
# MAGIC The following will provide a link to the web interface to explore individual run details and status. In the future we might support output displayed in the notebook.

# COMMAND ----------

displayHTML("<a href={} target='_blank'>Azure Portal: {}</a>".format(local_run.get_portal_url(), local_run.id))

# COMMAND ----------

# MAGIC %md The following will show the child runs and waits for the parent run to complete.

# COMMAND ----------

# MAGIC %md #### Retrieve All Child Runs after the experiment is completed (in portal)
# MAGIC You can also use SDK methods to fetch all the child runs and see individual metrics that we log.

# COMMAND ----------

children = list(local_run.get_children())
metricslist = {}
for run in children:
    properties = run.get_properties()
    #print(properties)
    metrics = {k: v for k, v in run.get_metrics().items() if isinstance(v, float)}    
    metricslist[int(properties['iteration'])] = metrics

rundata = pd.DataFrame(metricslist).sort_index(1)
rundata

# COMMAND ----------

# MAGIC %md ## Deploy
# MAGIC 
# MAGIC ### Retrieve the Best Model
# MAGIC 
# MAGIC Below we select the best pipeline from our iterations. The `get_output` method on `automl_classifier` returns the best run and the fitted model for the last invocation. Overloads on `get_output` allow you to retrieve the best run and fitted model for *any* logged metric or for a particular *iteration*.

# COMMAND ----------

best_run, fitted_model = local_run.get_output()

# COMMAND ----------

# MAGIC %md ### Download the conda environment file
# MAGIC From the *best_run* download the conda environment file that was used to train the AutoML model.

# COMMAND ----------

from azureml.automl.core.shared import constants
conda_env_file_name = 'conda_env.yml'
best_run.download_file(name="outputs/conda_env_v_1_0_0.yml", output_file_path=conda_env_file_name)
with open(conda_env_file_name, "r") as conda_file:
    conda_file_contents = conda_file.read()
    print(conda_file_contents)

# COMMAND ----------

# MAGIC %md ### Download the model scoring file
# MAGIC From the *best_run* download the scoring file to get the predictions from the AutoML model.

# COMMAND ----------

from azureml.automl.core.shared import constants
script_file_name = 'scoring_file.py'
best_run.download_file(name="outputs/scoring_file_v_1_0_0.py", output_file_path=script_file_name)
with open(script_file_name, "r") as scoring_file:
    scoring_file_contents = scoring_file.read()
    print(scoring_file_contents)

# COMMAND ----------

# MAGIC %md ## Register the Fitted Model for Deployment
# MAGIC If neither metric nor iteration are specified in the register_model call, the iteration with the best primary metric is registered.

# COMMAND ----------

description = 'AutoML Model'
tags = None
model = local_run.register_model(description = description, tags = tags)
local_run.model_id # This will be written to the scoring script file later in the notebook.

# COMMAND ----------

# MAGIC %md ### Deploy the model as a Web Service on Azure Container Instance
# MAGIC 
# MAGIC Create the configuration needed for deploying the model as a web service service.

# COMMAND ----------

from azureml.core.model import InferenceConfig
from azureml.core.webservice import AciWebservice
from azureml.core.environment import Environment

myenv = Environment.from_conda_specification(name="myenv", file_path=conda_env_file_name)
inference_config = InferenceConfig(entry_script=script_file_name, environment=myenv)

aciconfig = AciWebservice.deploy_configuration(cpu_cores = 1, 
                                               memory_gb = 1, 
                                               tags = {'area': "digits", 'type': "automl_classification"}, 
                                               description = 'sample service for Automl Classification')

# COMMAND ----------

from azureml.core.webservice import Webservice
from azureml.core.model import Model

aci_service_name = 'automl-databricks-local'
print(aci_service_name)
aci_service = Model.deploy(ws, aci_service_name, [model], inference_config, aciconfig)
aci_service.wait_for_deployment(True)
print(aci_service.state)

# COMMAND ----------

# MAGIC %md ### Test the Best Fitted Model
# MAGIC 
# MAGIC #### Load Test Data - you can split the dataset beforehand & pass Train dataset to AutoML and use Test dataset to evaluate the best model.

# COMMAND ----------

blob_location = "https://{}.blob.core.windows.net/{}".format(account_name, container_name)
X_test = pd.read_csv("{}./X_valid.csv".format(blob_location), header=0)
y_test = pd.read_csv("{}/y_valid.csv".format(blob_location), header=0)
images  = pd.read_csv("{}/images.csv".format(blob_location), header=None)
images = np.reshape(images.values, (100,8,8))

# COMMAND ----------

# MAGIC %md #### Testing Our Best Fitted Model
# MAGIC We will try to predict digits and see how our model works. This is just an example to show you.

# COMMAND ----------

import json
# Randomly select digits and test.
for index in np.random.choice(len(y_test), 2, replace = False):
    print(index)
    test_sample = json.dumps({'data':X_test[index:index + 1].values.tolist()})
    predicted = aci_service.run(input_data = test_sample)
    label = y_test.values[index]
    predictedDict = json.loads(predicted)
    title = "Label value = %d  Predicted value = %s " % ( label,predictedDict['result'][0])    
    fig = plt.figure(3, figsize = (5,5))
    ax1 = fig.add_axes((0,0,.8,.8))
    ax1.set_title(title)
    plt.imshow(images[index], cmap = plt.cm.gray_r, interpolation = 'nearest')
    display(fig)

# COMMAND ----------

### Delete the service

# COMMAND ----------

myservice.delete()

# COMMAND ----------

# MAGIC %md ![Impressions](https://PixelServer20190423114238.azurewebsites.net/api/impressions/MachineLearningNotebooks/how-to-use-azureml/azure-databricks/automl/automl-databricks-local-with-deployment.png)