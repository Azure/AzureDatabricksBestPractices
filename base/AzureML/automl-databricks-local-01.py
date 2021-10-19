# Databricks notebook source
# MAGIC %md Copyright (c) Microsoft Corporation. All rights reserved.
# MAGIC 
# MAGIC Licensed under the MIT License.

# COMMAND ----------

# MAGIC %md # Automated ML on Azure Databricks
# MAGIC 
# MAGIC In this example we use the scikit-learn's <a href="http://scikit-learn.org/stable/datasets/index.html#optical-recognition-of-handwritten-digits-dataset" target="_blank">digit dataset</a> to showcase how you can use AutoML for a simple classification problem.
# MAGIC 
# MAGIC In this notebook you will learn how to:
# MAGIC 1. Create Azure Machine Learning Workspace object and initialize your notebook directory to easily reload this object from a configuration file.
# MAGIC 2. Create an `Experiment` in an existing `Workspace`.
# MAGIC 3. Configure Automated ML using `AutoMLConfig`.
# MAGIC 4. Train the model using Azure Databricks.
# MAGIC 5. Explore the results.
# MAGIC 6. Viewing the engineered names for featurized data and featurization summary for all raw features.
# MAGIC 7. Test the best fitted model.
# MAGIC 
# MAGIC Before running this notebook, please follow the <a href="https://github.com/Azure/MachineLearningNotebooks/tree/master/how-to-use-azureml/azure-databricks" target="_blank">readme for using Automated ML on Azure Databricks</a> for installing necessary libraries to your cluster.

# COMMAND ----------

# MAGIC %md We support installing AML SDK with Automated ML as library from GUI. When attaching a library follow <a href="https://docs.databricks.com/user-guide/libraries.html" target="_blank">this link</a> and add the below string as your PyPi package. You can select the option to attach the library to all clusters or just one cluster.
# MAGIC 
# MAGIC **azureml-sdk with automated ml**
# MAGIC * Source: Upload Python Egg or PyPi
# MAGIC * PyPi Name: `azureml-sdk[automl]`
# MAGIC * Select Install Library

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
workspace_location = "eastus2"
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
                      location = workspace_location,                      
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
# MAGIC As part of the setup you have already created an Azure ML `Workspace` object. For Automated ML you will need to create an `Experiment` object, which is a named object in a `Workspace` used to run experiments.

# COMMAND ----------

# MAGIC %sh pip install pandas==0.22.0

# COMMAND ----------

import logging
import os
import random
import time

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

# X_train.take(5).to_pandas_dataframe()
display(X_train.take(5).to_pandas_dataframe())

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
                             iterations = 3,
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

# MAGIC %md ## Continue experiment

# COMMAND ----------

local_run.continue_experiment(iterations=2,
                              X=X_train, 
                              y=y_train,
                              spark_context=sc,
                              show_output=True)

# COMMAND ----------

# MAGIC %md ## Explore the Results

# COMMAND ----------

# MAGIC %md #### Portal URL for Monitoring Runs
# MAGIC 
# MAGIC The following will provide a link to the web interface to explore individual run details and status. In the future we might support output displayed in the notebook.

# COMMAND ----------

displayHTML("<a href={} target='_blank'>Your experiment in Azure Portal: {}</a>".format(local_run.get_portal_url(), local_run.id))

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
    metrics = {k: v for k, v in run.get_metrics().items() if isinstance(v, float)}    
    metricslist[int(properties['iteration'])] = metrics

rundata = pd.DataFrame(metricslist).sort_index(1)
rundata

# COMMAND ----------

# MAGIC %md ### Retrieve the Best Model after the above run is complete 
# MAGIC 
# MAGIC Below we select the best pipeline from our iterations. The `get_output` method returns the best run and the fitted model. The Model includes the pipeline and any pre-processing.  Overloads on `get_output` allow you to retrieve the best run and fitted model for *any* logged metric or for a particular *iteration*.

# COMMAND ----------

best_run, fitted_model = local_run.get_output()
print(best_run)
print(fitted_model)

# COMMAND ----------

# MAGIC %md #### Best Model Based on Any Other Metric after the above run is complete based on the child run
# MAGIC Show the run and the model that has the smallest `log_loss` value:

# COMMAND ----------

lookup_metric = "log_loss"
best_run, fitted_model = local_run.get_output(metric = lookup_metric)
print(best_run)
print(fitted_model)

# COMMAND ----------

# MAGIC %md #### View the engineered names for featurized data
# MAGIC Below we display the engineered feature names generated for the featurized data using the preprocessing featurization.

# COMMAND ----------

fitted_model.named_steps['datatransformer'].get_engineered_feature_names()

# COMMAND ----------

# MAGIC %md #### View the featurization summary
# MAGIC Below we display the featurization that was performed on different raw features in the user data. For each raw feature in the user data, the following information is displayed:-
# MAGIC - Raw feature name
# MAGIC - Number of engineered features formed out of this raw feature
# MAGIC - Type detected
# MAGIC - If feature was dropped
# MAGIC - List of feature transformations for the raw feature

# COMMAND ----------

# Get the featurization summary as a list of JSON
featurization_summary = fitted_model.named_steps['datatransformer'].get_featurization_summary()
# View the featurization summary as a pandas dataframe
pd.DataFrame.from_records(featurization_summary)

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

# Randomly select digits and test.
for index in np.random.choice(len(y_test), 2, replace = False):
    print(index)
    predicted = fitted_model.predict(X_test[index:index + 1])[0]
    label = y_test.values[index]
    title = "Label value = %d  Predicted value = %d " % (label, predicted)
    fig = plt.figure(3, figsize = (5,5))
    ax1 = fig.add_axes((0,0,.8,.8))
    ax1.set_title(title)
    plt.imshow(images[index], cmap = plt.cm.gray_r, interpolation = 'nearest')
    display(fig)

# COMMAND ----------

# MAGIC %md When deploying an automated ML trained model, please specify _pippackages=['azureml-sdk[automl]']_ in your CondaDependencies.
# MAGIC 
# MAGIC Please refer to only the **Deploy** section in this notebook - <a href="https://github.com/Azure/MachineLearningNotebooks/blob/master/how-to-use-azureml/automated-machine-learning/classification-with-deployment" target="_blank">Deployment of Automated ML trained model</a>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ![Impressions](https://PixelServer20190423114238.azurewebsites.net/api/impressions/MachineLearningNotebooks/how-to-use-azureml/azure-databricks/automl/automl-databricks-local-01.png)