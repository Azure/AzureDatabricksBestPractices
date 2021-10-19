# Databricks notebook source
# MAGIC %md # AutoML on Databricks Cluster (Porto Seguro's Safe Driving Prediction)
# MAGIC 
# MAGIC This notebook is refactored (from the original AutoML local training notebook) to use AutoML on Azure Databricks cluster.
# MAGIC It also uses AML Datasets for training instead of Pandas Dataframes.

# COMMAND ----------

# MAGIC %md ## Import Needed Packages
# MAGIC 
# MAGIC Import the packages needed for this notebook. The most widely used package for machine learning is [scikit-learn](https://scikit-learn.org/stable/), [pandas](https://pandas.pydata.org/docs/getting_started/index.html#getting-started), and [numpy](https://numpy.org/). These packages have various features, as well as a lot of clustering, regression and classification algorithms that make it a good choice for data mining and data analysis.

# COMMAND ----------

import numpy as np
import pandas as pd
import joblib
from sklearn import metrics

# COMMAND ----------

import azureml.core
print("You are currently using version", azureml.core.VERSION, "of the Azure ML SDK")

# COMMAND ----------

# MAGIC %md ##  Get Azure ML Workspace to use

# COMMAND ----------

from azureml.core import Workspace, Dataset
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

# MAGIC %md ## Load data into Azure ML Dataset and Register into Workspace

# COMMAND ----------

# Try to load the dataset from the Workspace. Otherwise, create it from the file in the HTTP URL
found = False
aml_dataset_name = "safedriverdataset"

if aml_dataset_name in ws.datasets.keys(): 
       found = True
       aml_dataset = ws.datasets[aml_dataset_name] 
       print("Dataset loaded from the Workspace")
       
if not found:
        # Create AML Dataset and register it into Workspace
        print("Dataset does not exist in the current Workspace. It will be imported and registered.")
        
        # Option A: Create AML Dataset from file in AML DataStore
        datastore = ws.get_default_datastore()
        aml_dataset = Dataset.Tabular.from_delimited_files(path=datastore.path('Datasets/porto_seguro_safe_driver_prediction/porto_seguro_safe_driver_prediction_train.csv'))
        data_origin_type = 'AMLDataStore'
        
        # Option B: Create AML Dataset from file in HTTP URL
        # data_url = 'https://azmlworkshopdata.blob.core.windows.net/safedriverdata/porto_seguro_safe_driver_prediction_train.csv'
        # aml_dataset = Dataset.Tabular.from_delimited_files(data_url)  
        # data_origin_type = 'HttpUrl'
        
        print(aml_dataset)
                
        #Register Dataset in Workspace
        registration_method = 'SDK'  # or 'UI'
        aml_dataset = aml_dataset.register(workspace=ws,
                                           name=aml_dataset_name,
                                           description='Porto Seguro Safe Driver Prediction Train dataset file',
                                           tags={'Registration-Method': registration_method, 'Data-Origin-Type': data_origin_type},
                                           create_new_version=True)
        
        print("Dataset created from file and registered in the Workspace")


# COMMAND ----------

# Use Pandas DataFrame just to sneak peak some data and schema
data_df = aml_dataset.to_pandas_dataframe()
print(data_df.shape)
# print(data_df.describe())
data_df.head(5)

# COMMAND ----------

# MAGIC %md ## Split Data into Train and Test AML Tabular Datasets
# MAGIC 
# MAGIC Remote AML Training you need to use AML Datasets, you cannot submit Pandas Dataframes to remote runs of AutoMLConfig.
# MAGIC 
# MAGIC Note that AutoMLConfig below is not using the Test dataset (you only provide a single dataset that will internally be split in validation/train datasets or use cross-validation depending on the size of the dataset. The boundary for that is 20k rows, using cross-validation if less than 20k. This can also be decided by the user.). 
# MAGIC 
# MAGIC The Test dataset will be used at the end of the notebook to manually calculate the quality metrics with a dataset not seen by AutoML training.

# COMMAND ----------

# Split in train/test datasets (Test=10%, Train=90%)

train_dataset, test_dataset = aml_dataset.random_split(0.9, seed=0)

# Use Pandas DF only to check the data
train_df = train_dataset.to_pandas_dataframe()
test_df = test_dataset.to_pandas_dataframe()

# COMMAND ----------

print(train_df.shape)
print(test_df.shape)

train_df.describe()

# COMMAND ----------

train_df.head(5)

# COMMAND ----------

# MAGIC %md ## Run on Databricks Cluster
# MAGIC ### You can use the default running cluster or attach to another databricks cluster that is linked in aml compute

# COMMAND ----------

from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException

# Choose a name for your CPU cluster
dbx_cluster_name = "dbxcluster"

dbx_cluster = ComputeTarget(workspace=ws, name=dbx_cluster_name)


# COMMAND ----------

# MAGIC %md ## Train with Azure AutoML automatically searching for the 'best model' (Best algorithms and best hyper-parameters)

# COMMAND ----------

# MAGIC %md ### List and select primary metric to drive the AutoML classification problem

# COMMAND ----------

from azureml.train import automl

# List of possible primary metrics is here:
# https://docs.microsoft.com/en-us/azure/machine-learning/how-to-configure-auto-train#primary-metric
    
# Get a list of valid metrics for your given task
automl.utilities.get_primary_metrics('classification')

# COMMAND ----------

# MAGIC %md ## Define AutoML Experiment settings

# COMMAND ----------

# MAGIC %md ## Train
# MAGIC 
# MAGIC Instantiate a AutoMLConfig object. This defines the settings and data used to run the experiment.
# MAGIC 
# MAGIC |Property|Description|
# MAGIC |-|-|
# MAGIC |**task**|classification or regression or forecasting|
# MAGIC |**primary_metric**|This is the metric that you want to optimize. Classification supports the following primary metrics: <br><i>accuracy</i><br><i>AUC_weighted</i><br><i>average_precision_score_weighted</i><br><i>norm_macro_recall</i><br><i>precision_score_weighted</i>|
# MAGIC |**iteration_timeout_minutes**|Time limit in minutes for each iteration.|
# MAGIC |**blacklist_models** | *List* of *strings* indicating machine learning algorithms for AutoML to avoid in this run. <br><br> Allowed values for **Classification**<br><i>LogisticRegression</i><br><i>SGD</i><br><i>MultinomialNaiveBayes</i><br><i>BernoulliNaiveBayes</i><br><i>SVM</i><br><i>LinearSVM</i><br><i>KNN</i><br><i>DecisionTree</i><br><i>RandomForest</i><br><i>ExtremeRandomTrees</i><br><i>LightGBM</i><br><i>GradientBoosting</i><br><i>TensorFlowDNN</i><br><i>TensorFlowLinearClassifier</i><br><br>Allowed values for **Regression**<br><i>ElasticNet</i><br><i>GradientBoosting</i><br><i>DecisionTree</i><br><i>KNN</i><br><i>LassoLars</i><br><i>SGD</i><br><i>RandomForest</i><br><i>ExtremeRandomTrees</i><br><i>LightGBM</i><br><i>TensorFlowLinearRegressor</i><br><i>TensorFlowDNN</i><br><br>Allowed values for **Forecasting**<br><i>ElasticNet</i><br><i>GradientBoosting</i><br><i>DecisionTree</i><br><i>KNN</i><br><i>LassoLars</i><br><i>SGD</i><br><i>RandomForest</i><br><i>ExtremeRandomTrees</i><br><i>LightGBM</i><br><i>TensorFlowLinearRegressor</i><br><i>TensorFlowDNN</i><br><i>Arima</i><br><i>Prophet</i>|
# MAGIC | **whitelist_models** |  *List* of *strings* indicating machine learning algorithms for AutoML to use in this run. Same values listed above for **blacklist_models** allowed for **whitelist_models**.|
# MAGIC |**experiment_exit_score**| Value indicating the target for *primary_metric*. <br>Once the target is surpassed the run terminates.|
# MAGIC |**experiment_timeout_hours**| Maximum amount of time in hours that all iterations combined can take before the experiment terminates.|
# MAGIC |**enable_early_stopping**| Flag to enble early termination if the score is not improving in the short term.|
# MAGIC |**featurization**| 'auto' / 'off'  Indicator for whether featurization step should be done automatically or not. Note: If the input data is sparse, featurization cannot be turned on.|
# MAGIC |**n_cross_validations**|Number of cross validation splits.|
# MAGIC |**training_data**|Input dataset, containing both features and label column.|
# MAGIC |**label_column_name**|The name of the label column.|
# MAGIC 
# MAGIC **_You can find more information about primary metrics_** [here](https://docs.microsoft.com/en-us/azure/machine-learning/service/how-to-configure-auto-train#primary-metric)

# COMMAND ----------

# MAGIC %md
# MAGIC ### To make it run on Databricks cluster just add spark_context=sc

# COMMAND ----------

import logging

# You can provide additional settings as a **kwargs parameter for the AutoMLConfig object
automl_settings = {
      "blacklist_models":['LogisticRegression', 'ExtremeRandomTrees', 'RandomForest'], 
      # "whitelist_models": ['LightGBM'],
      "validation_size": 0.1,
      # "validation_data": validation_df,  # If you have an explicit validation set
      # "n_cross_validations": 5,
      # "experiment_exit_score": 0.7,
      # "max_cores_per_iteration": -1,
      "max_concurrent_iterations": 4,
      "enable_voting_ensemble": True,
      "experiment_timeout_hours" : 1,
      "enable_early_stopping" : True,
      "iteration_timeout_minutes": 5,
      "enable_stack_ensemble": True
}

from azureml.train.automl import AutoMLConfig

automl_config = AutoMLConfig(#compute_target=dbx_cluster,
                             task='classification',
                             primary_metric='AUC_weighted',                           
                             training_data=train_dataset, # AML Dataset
                             label_column_name="target",                                                    
                             iterations=15,                       
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

# MAGIC %md ## Run Experiment with multiple child runs under the covers

# COMMAND ----------

from azureml.core import Experiment

experiment_name = "dbx_porto_seguro_driver_pred_2"
print(experiment_name)

experiment = Experiment(workspace=ws, 
                        name=experiment_name)

# COMMAND ----------



import time
start_time = time.time()
            
run = experiment.submit(automl_config, show_output=False)

#Enable this timer if you have show_output=True
#print('Manual run timing: --- %s minutes needed for running the whole Remote AutoML Experiment ---' % ((time.time() - start_time)/60))


# COMMAND ----------

# MAGIC %md 
# MAGIC ### You can load an existing run by providing a run_id

# COMMAND ----------

import azureml.core
from azureml.core import Workspace, Experiment, Run
run_id="AutoML_3d3b3748-6aa6-4f1f-ba28-e62ed89267f3"

run = azureml.core.run.get_run(experiment, run_id, rehydrate=True)
run

# COMMAND ----------

# MAGIC %md ### Measure Parent Run Time needed for the whole AutoML process 

# COMMAND ----------

import time
from datetime import datetime

run_details = run.get_details()

# Like: 2020-01-12T23:11:56.292703Z
end_time_utc_str = run_details['endTimeUtc'].split(".")[0]
start_time_utc_str = run_details['startTimeUtc'].split(".")[0]
timestamp_end = time.mktime(datetime.strptime(end_time_utc_str, "%Y-%m-%dT%H:%M:%S").timetuple())
timestamp_start = time.mktime(datetime.strptime(start_time_utc_str, "%Y-%m-%dT%H:%M:%S").timetuple())

parent_run_time = timestamp_end - timestamp_start
print('Run Timing: --- %s minutes needed for running the whole Remote AutoML Experiment ---' % (parent_run_time/60))

# COMMAND ----------

# MAGIC %md ## Retrieve the 'Best' Model

# COMMAND ----------

best_run, fitted_model = run.get_output()
print(best_run)
print('--------')
print(fitted_model)

# COMMAND ----------

# MAGIC %md ## Register Model in Workspace model registry

# COMMAND ----------

registered_model = run.register_model(model_name='porto-seg-automl-remote-compute', 
                                      description='Porto Seguro Model from plain AutoML in remote AML compute')

print(run.model_id)
registered_model

# COMMAND ----------

# MAGIC %md ## See files associated with the 'Best run'

# COMMAND ----------

print(best_run.get_file_names())

# best_run.download_file('azureml-logs/70_driver_log.txt')

# COMMAND ----------

# MAGIC %md ## Make Predictions and calculate metrics

# COMMAND ----------

# MAGIC %md ### Prep Test Data: Extract X values (feature columns) from test dataset and convert to NumPi array for predicting 

# COMMAND ----------

import pandas as pd

x_test_df = test_df.copy()

if 'target' in x_test_df.columns:
    y_test_df = x_test_df.pop('target')

print(test_df.shape)
print(x_test_df.shape)
print(y_test_df.shape)

# COMMAND ----------

y_test_df.describe()

# COMMAND ----------

# MAGIC %md ### Make predictions in bulk

# COMMAND ----------

# Try the best model making predictions with the test dataset
y_predictions = fitted_model.predict(x_test_df)

print(y_predictions.shape)
print('30 predictions: ')
print(y_predictions[:30])

# COMMAND ----------

# MAGIC %md ### Get all the predictions' probabilities needed to calculate ROC AUC

# COMMAND ----------

class_probabilities = fitted_model.predict_proba(x_test_df)
print(class_probabilities.shape)

print('Some class probabilities...: ')
print(class_probabilities[:3])

print('Probabilities for class 1:')
print(class_probabilities[:,1])

print('Probabilities for class 0:')
print(class_probabilities[:,0])

# COMMAND ----------

# MAGIC %md ## Evaluate Model
# MAGIC 
# MAGIC Evaluating performance is an essential task in machine learning. In this case, because this is a classification problem, the data scientist elected to use an AUC - ROC Curve. When we need to check or visualize the performance of the multi - class classification problem, we use AUC (Area Under The Curve) ROC (Receiver Operating Characteristics) curve. It is one of the most important evaluation metrics for checking any classification model’s performance.
# MAGIC 
# MAGIC <img src="https://www.researchgate.net/profile/Oxana_Trifonova/publication/276079439/figure/fig2/AS:614187332034565@1523445079168/An-example-of-ROC-curves-with-good-AUC-09-and-satisfactory-AUC-065-parameters.png"
# MAGIC      alt="Markdown Monster icon"
# MAGIC      style="float: left; margin-right: 12px; width: 320px; height: 239px;" />

# COMMAND ----------

# MAGIC %md ### Calculate the ROC AUC with probabilities vs. the Test Dataset

# COMMAND ----------

print('ROC AUC *method 1*:')
fpr, tpr, thresholds = metrics.roc_curve(y_test_df, class_probabilities[:,1])
metrics.auc(fpr, tpr)

# COMMAND ----------

from sklearn.metrics import roc_auc_score

print('ROC AUC *method 2*:')
print(roc_auc_score(y_test_df, class_probabilities[:,1]))

print('ROC AUC Weighted:')
print(roc_auc_score(y_test_df, class_probabilities[:,1], average='weighted'))
# AUC with plain LightGBM was: 0.6374553321494826 

# COMMAND ----------

# MAGIC %md ### Calculate the Accuracy with predictions vs. the Test Dataset

# COMMAND ----------

print(y_test_df.shape)
print(y_predictions.shape)

# COMMAND ----------

from sklearn.metrics import accuracy_score

print('Accuracy:')
print(accuracy_score(y_test_df, y_predictions))


# COMMAND ----------

# MAGIC %md ### Load model in memory

# COMMAND ----------

# MAGIC %md #### (Option A: Load from model .pkl file)

# COMMAND ----------

# Load the model into memory from downloaded file
import joblib

fitted_model_pkl = joblib.load('model.pkl')
print(fitted_model_pkl)

# COMMAND ----------

# MAGIC %md #### (Option B: Load from model registry in Workspace)

# COMMAND ----------

# Load model from model registry in Workspace
from azureml.core.model import Model

# model_from_reg = Model(ws, 'porto-seg-automl-remote-compute')

name_model_from_plain_automl = 'porto-seg-automl-remote-compute'
name_model_from_pipeline_automlstep = 'porto-model-from-automlstep'

model_path = Model.get_model_path(name_model_from_pipeline_automlstep, _workspace=ws)
fitted_model = joblib.load(model_path)
print(fitted_model)

# COMMAND ----------

# MAGIC %md ## Try model inference with hardcoded input data for the model to predict

# COMMAND ----------

# Data from Dataframe for comparison with hardcoded data
x_test_df.head(1)

# COMMAND ----------

# Data from Dataframe for comparison with hardcoded data
print(x_test_df.head(1).values)
print(x_test_df.head(1).columns)

# COMMAND ----------

import json

raw_data = json.dumps({
     'data': [[20,2,1,3,1,0,0,1,0,0,0,0,0,0,0,8,1,0,0,0.6,0.1,0.61745445,6,1,-1,0,1,11,1,1,0,1,99,2,0.31622777,0.6396829,0.36878178,3.16227766,0.2,0.6,0.5,2,2,8,1,8,3,10,3,0,0,10,0,1,0,0,1,0]],
     'method': 'predict'  # If you have a classification model, you can get probabilities by changing this to 'predict_proba'.
 })

print(json.loads(raw_data)['data'])

numpy_data = np.array(json.loads(raw_data)['data'])

df_data = pd.DataFrame(data=numpy_data, columns=['id', 'ps_ind_01', 'ps_ind_02_cat', 'ps_ind_03', 'ps_ind_04_cat',
                                               'ps_ind_05_cat', 'ps_ind_06_bin', 'ps_ind_07_bin', 'ps_ind_08_bin',
                                               'ps_ind_09_bin', 'ps_ind_10_bin', 'ps_ind_11_bin', 'ps_ind_12_bin',
                                               'ps_ind_13_bin', 'ps_ind_14', 'ps_ind_15', 'ps_ind_16_bin',
                                               'ps_ind_17_bin', 'ps_ind_18_bin', 'ps_reg_01', 'ps_reg_02', 'ps_reg_03',
                                               'ps_car_01_cat', 'ps_car_02_cat', 'ps_car_03_cat', 'ps_car_04_cat',
                                               'ps_car_05_cat', 'ps_car_06_cat', 'ps_car_07_cat', 'ps_car_08_cat',
                                               'ps_car_09_cat', 'ps_car_10_cat', 'ps_car_11_cat', 'ps_car_11',
                                               'ps_car_12', 'ps_car_13', 'ps_car_14', 'ps_car_15', 'ps_calc_01',
                                               'ps_calc_02', 'ps_calc_03', 'ps_calc_04', 'ps_calc_05', 'ps_calc_06',
                                               'ps_calc_07', 'ps_calc_08', 'ps_calc_09', 'ps_calc_10', 'ps_calc_11',
                                               'ps_calc_12', 'ps_calc_13', 'ps_calc_14', 'ps_calc_15_bin',
                                               'ps_calc_16_bin', 'ps_calc_17_bin', 'ps_calc_18_bin', 'ps_calc_19_bin',
                                               'ps_calc_20_bin'])
df_data

# COMMAND ----------

# Get predictions from the model
y_predictions = fitted_model.predict(df_data) # x_test_df.head(1)
y_predictions # Should return a [0] or [1] depending on the prediction result

# COMMAND ----------

best_run_customized, fitted_model_customized = run.get_output()

# COMMAND ----------

# MAGIC %md ## Transparency
# MAGIC 
# MAGIC View updated featurization summary

# COMMAND ----------

custom_featurizer = fitted_model_customized.named_steps['datatransformer']
df = custom_featurizer.get_featurization_summary()
pd.DataFrame(data=df)

# COMMAND ----------

# MAGIC %md ### Retrieve the Best Model's explanation
# MAGIC Retrieve the explanation from the best_run which includes explanations for engineered features and raw features. Make sure that the run for generating explanations for the best model is completed.

# COMMAND ----------

# Wait for the best model explanation run to complete
from azureml.core.run import Run
model_explainability_run_id = run.get_properties().get('ModelExplainRunId')
print(model_explainability_run_id)
if model_explainability_run_id is not None:
    model_explainability_run = Run(experiment=experiment, run_id=model_explainability_run_id)
    model_explainability_run.wait_for_completion()

# Get the best run object
best_run, fitted_model = run.get_output()

# COMMAND ----------

best_run

# COMMAND ----------

fitted_model

# COMMAND ----------

# MAGIC %md #### Download engineered feature importance from artifact store
# MAGIC You can use ExplanationClient to download the engineered feature explanations from the artifact store of the best_run.

# COMMAND ----------

from azureml.explain.model._internal.explanation_client import ExplanationClient

client = ExplanationClient.from_run(best_run)
engineered_explanations = client.download_model_explanation(raw=False)
exp_data = engineered_explanations.get_feature_importance_dict()
exp_data

# COMMAND ----------

# MAGIC %md #### Download raw feature importance from artifact store
# MAGIC You can use ExplanationClient to download the raw feature explanations from the artifact store of the best_run.

# COMMAND ----------

client = ExplanationClient.from_run(best_run)
engineered_explanations = client.download_model_explanation(raw=True)
exp_data = engineered_explanations.get_feature_importance_dict()
exp_data

# COMMAND ----------

# MAGIC %md ## Deploy
# MAGIC 
# MAGIC ### Retrieve the Best Model
# MAGIC 
# MAGIC Below we select the best pipeline from our iterations.  The `get_output` method returns the best run and the fitted model. Overloads on `get_output` allow you to retrieve the best run and fitted model for *any* logged metric or for a particular *iteration*.

# COMMAND ----------

best_run, fitted_model = run.get_output()

# COMMAND ----------

model_name = best_run.properties['model_name']

script_file_name = 'inference/score.py'
conda_env_file_name = 'inference/env.yml'

best_run.download_file('outputs/scoring_file_v_1_0_0.py', 'inference/score.py')
best_run.download_file('outputs/conda_env_v_1_0_0.yml', 'inference/env.yml')

# COMMAND ----------

# MAGIC %md ### Register the Fitted Model for Deployment
# MAGIC If neither `metric` nor `iteration` are specified in the `register_model` call, the iteration with the best primary metric is registered.

# COMMAND ----------

description = 'AutoML Model trained'
tags = None
model = run.register_model(model_name = model_name, description = description, tags = tags)

print(run.model_id) # This will be written to the script file later in the notebook.

# COMMAND ----------

# MAGIC %md ### Deploy the model as a Web Service on Azure Container Instance

# COMMAND ----------

from azureml.core.model import InferenceConfig
from azureml.core.webservice import AciWebservice
from azureml.core.webservice import Webservice
from azureml.core.model import Model
from azureml.core.environment import Environment

myenv = Environment.from_conda_specification(name="myenv", file_path=conda_env_file_name)
inference_config = InferenceConfig(entry_script=script_file_name, environment=myenv)

aciconfig = AciWebservice.deploy_configuration(cpu_cores = 1, 
                                               memory_gb = 1, 
                                               tags = {'area': "bmData", 'type': "automl_classification"}, 
                                               description = 'Porto Seguro for Automl Classification')

aci_service_name = 'automl-porto-safe-driver'
print(aci_service_name)
aci_service = Model.deploy(ws, aci_service_name, [model], inference_config, aciconfig)
aci_service.wait_for_deployment(True)
print(aci_service.state)

# COMMAND ----------

