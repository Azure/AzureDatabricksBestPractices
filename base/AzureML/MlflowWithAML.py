# Databricks notebook source
# MAGIC %md
# MAGIC #Experiment Tracking and Model Deployment 
# MAGIC ##with MLFlow and Azure Machine Learning
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/Ignite/intro.jpg" alt="Better Together" width="800">
# MAGIC </br></br>
# MAGIC This notebook walks through a basic Machine Learning example. Training runs will be logged to Azure Machine Learning using MLFlow's open-source APIs.  </br> A resulting model from one of the models will then be deployed using MLFlow APIs as </br>a) a Spark Pandas UDF for batch scoring and </br>b) a web service in Azure Machine Learning
# MAGIC 
# MAGIC <b>Ensure you have linked databricks to an AzureML workspace before proceeding</b>

# COMMAND ----------

# MAGIC %md ![Workspace](https://github.com/parasharshah/automl-handson/raw/master/image4deploy.JPG)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Import and set up Training Data
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/Ignite/delta.jpg" alt="Delta" width="600">
# MAGIC </br></br>
# MAGIC The training data for this notebook is simply some time series data from devices that includes a collection of sensor readings.  
# MAGIC The data is stored in the Delta Lake format.  The data can be found [here](https://mcg1stanstor00.blob.core.windows.net/publicdata/sensors/sensordata.csv).

# COMMAND ----------

# MAGIC %md 
# MAGIC <b> Download and store data in a table called `Sensors` </b>

# COMMAND ----------

# DBTITLE 0,Download and setup data
import urllib

spark.sql("drop table if exists Sensors")
urllib.request.urlretrieve("https://mcg1stanstor00.blob.core.windows.net/publicdata/sensors/sensordata.csv","/tmp/sensordata.csv") 
dbutils.fs.mv("file:/tmp/sensordata.csv", "dbfs:/data/sensordata.csv")
spark.read.csv("dbfs:/data/sensordata.csv", inferSchema = True, header = True).write.format('delta').saveAsTable('Sensors')

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Examine data in table</b>

# COMMAND ----------

dataDf = spark.table('Sensors').where(col('Device') == 'Device001')
display(dataDf.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Experiment Tracking with MLFlow and AML
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/Ignite/experiment.jpg" alt="Experiment Tracking" width="750">
# MAGIC </br>
# MAGIC MLFlow logging APIs will be used to log training experiments, metrics, and artifacts to AML.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Import required libraries</b>

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import random
import string
import mlflow
import mlflow.spark
import mlflow.sklearn
import mlflow.azureml
import azureml
import azureml.mlflow
import azureml.core
from azureml.core import Workspace
from azureml.mlflow import get_portal_url

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/Ignite/spark.jpg" alt="Spark" width="150">
# MAGIC <b> Train using Spark</b> 

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

with mlflow.start_run() as spark_run:  #start the training run

  assembler = VectorAssembler(
      inputCols=["Sensor1", "Sensor2", "Sensor3", "Sensor4"],
      outputCol="features")

  rf = RandomForestRegressor(featuresCol="features", labelCol="Sensor5")

  pipeline = Pipeline(stages=[assembler, rf])

  # Split the data into training and test sets (30% held out for testing)
  (trainingData, testData) = dataDf.randomSplit([0.7, 0.3])

  model = pipeline.fit(trainingData)
  mlflow.spark.log_model(model, "model") #save the model to local (databricks) file system

  predictions = model.transform(testData)

  # Select (prediction, true label) and compute test error
  evaluator = RegressionEvaluator(
      labelCol="Sensor5", predictionCol="prediction", metricName="mse")
  mse = evaluator.evaluate(predictions)
  mlflow.log_metric("mse", mse) #log the mse
  
  evaluator = RegressionEvaluator(
      labelCol="Sensor5", predictionCol="prediction", metricName="r2")
  r2 = evaluator.evaluate(predictions) 
  mlflow.log_metric("r2", r2) #log the mse

print(mse)
print(r2)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/Ignite/skl.jpg" alt="SciKit Learn" width="150">
# MAGIC <b> Train using scikit learn</b> 

# COMMAND ----------

import pandas as pd
from sklearn.linear_model import LinearRegression, Lasso
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor

#Setup Test/Train datasets
data = dataDf.toPandas()

x = data.drop(["Device", "Time", "Sensor5"], axis=1)
y = data[["Sensor5"]]
train_x, test_x, train_y, test_y = train_test_split(x,y,test_size=0.20, random_state=30)

#Train Models
device = "Device001"
run = 0

for i in [2, 10, 20]:
  with mlflow.start_run(run_name = str(i)) as sklearn_run: #start the run
    
    # Set the depth of each tree in the Random Forest Regressor
    maxDepth = i
    
   # Fit, train, and score the model
    model = RandomForestRegressor(max_depth = maxDepth)
    model.fit(train_x, train_y)
    preds = model.predict(test_x)

    # Log Model
    mlflow.sklearn.log_model(model, "model") #save the model to local file system

    # Get Metrics
    mse = mean_squared_error(test_y, preds)
    r2 = r2_score(test_y, preds)

    # Log Metrics
    mlflow.log_metric('mse', mse)
    mlflow.log_metric('r2', r2)

    # Build Metrics Table
    results = [[device, maxDepth, mse, r2]]
    runResultsPdf = pd.DataFrame(results, columns =['Device', 'MaxDepth', 'MSE', 'r2'])

    if run == 0:
      resultsPdf = runResultsPdf
    else:
      resultsPdf = resultsPdf.append(runResultsPdf)

    run =+ 1

    mlflow.end_run()
  
display(resultsPdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Deployment
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/Ignite/deploy.jpg" alt="Model Deployment" width="800">
# MAGIC </br></br>
# MAGIC Using MLFlow APIs, models can be deployed to AML and turned into web services, or they can be deployed as MLFlow model objects 
# MAGIC </br>and used in streaming or batch pipelines as Python functions or Pandas UDFs.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Deploy Model as Function for Batch Scoring

# COMMAND ----------

#Build the artifact URI for the model to deploy
runid = sklearn_run.info.run_id
model_uri='runs:/{}/model'.format(runid)
print(model_uri)

deployment_stub = ''.join([random.choice(string.ascii_lowercase) for i in range(5)]) #Lets create a new folder name each time so that we dont run into crazy dbfs issues while re-running the code
mytag = 'mlflowdemo' 

#Deploy the model objects from AML to a common object store
deployModel = mlflow.sklearn.load_model(model_uri) #loads model using the mlflow run
mlflow.sklearn.save_model(deployModel, '/dbfs/mnt/mcgen2/models/' + mytag + deployment_stub) #save model in a friendly place for running it on databricks
dbutils.fs.ls('/mnt/mcgen2/models/' + mytag + deployment_stub)

# COMMAND ----------

from pyspark.sql.types import ArrayType, FloatType

#Create a Spark UDF for the MLFlow model
pyfunc_udf = mlflow.pyfunc.spark_udf(spark, 'dbfs:/mnt/mcgen2/models/' + mytag + deployment_stub)

#Execute the UDF against some data
preds = (dataDf
#           .drop('Sensor5')
           .withColumn('Sensor5-prediction', pyfunc_udf('Sensor1', 'Sensor2', 'Sensor3', 'Sensor4'))
        )
#display the predictions
display(preds)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Deploy Model as Web Service in AML

# COMMAND ----------

#connect to the AML workspace
ws_name = '<aml workspace name>' #replace with your AML workspace_name
res_group = '<aml workspace resource group>' #replace with your AML workspace resource group
subs_id = '<subscription ID>' #replace with your AML workspace subscription ID

ws = Workspace.get(ws_name, resource_group=res_group, subscription_id=subs_id)

# COMMAND ----------

#this will take 5-10 minutes to complete
#Create the model and docker image in AML
model_image, azure_model = mlflow.azureml.build_image(model_uri="dbfs:/mnt/mcgen2/models/" + mytag + deployment_stub, 
                                                      workspace=ws,
                                                      model_name=mytag + "-model",
                                                      image_name=mytag + "-image",
                                                      synchronous=False)

model_image.wait_for_creation(show_output=True)

# COMMAND ----------

from azureml.core.webservice import AciWebservice, Webservice

#Create the web service in AML
webservice_name = mytag + "-service"

# COMMAND ----------

# Remove existing service so that we can create a new one
try:
    Webservice(ws, webservice_name).delete()
except:
    pass

# COMMAND ----------

#deploy the image which was registered in Azure ML (the one which was returend when we built the image)
webservice_deployment_config = AciWebservice.deploy_configuration(cpu_cores=1, memory_gb=1)
webservice = Webservice.deploy_from_image(name=webservice_name, image=model_image, deployment_config=webservice_deployment_config, workspace=ws)
webservice.wait_for_deployment(show_output=True)

# COMMAND ----------

#get a handle to the already deployed service
webservice = ws.webservices[webservice_name]

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Predict Using Web Service URI

# COMMAND ----------

##Get the Web Service URI 
uri = webservice.scoring_uri
print(uri)

# COMMAND ----------

# Create input data for the API
sample_json = {
    "columns": [
        "Sensor1",
        "Sensor2",
        "Sensor3",
        "Sensor4"
    ],
    "data": [
        [65.7845, 16613.676, 101.69767,	60.329124]
    ]
}

print(sample_json)

# COMMAND ----------

import requests
import json

# Function for calling the API
def service_query(input_data):
  response = requests.post(
              url=uri, data=json.dumps(input_data),
              headers={"Content-type": "application/json"})
  prediction = response.text
  print(prediction)
  return prediction

# API Call
service_query(sample_json)