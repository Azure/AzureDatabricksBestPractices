# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Real Time Deployment
# MAGIC 
# MAGIC While real time deployment represents a smaller share of the deployment landscape, many of these deployments represent high value tasks.  This lesson surveys real time deployment options ranging from proofs of concept to both custom and managed solutions with a focus on RESTful services.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Survey the landscape of real time deployment options
# MAGIC  - Prototype a RESTful service using MLflow
# MAGIC  - Walk through the deployment of REST endpoint using SageMaker
# MAGIC  - Query a REST endpoint for inference using individual records and batch requests

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### The Why and How of Real Time Deployment
# MAGIC 
# MAGIC Real time inference is generating predictions for a small number of records with fast results (e.g. results in milliseconds).  The first question to ask when considering real time deployment is: do I need it?  While real time deployment attracts a lot of attention, it represents a minority of machine learning inference use cases and one of the more complicated ways of deploying models.  That being said, domains where real time deployment is often needed are often of great business value.  These domains include:<br><br>
# MAGIC 
# MAGIC  - Financial services (especially with fraud detection)
# MAGIC  - Mobile
# MAGIC  - Adtech
# MAGIC 
# MAGIC There are a number of ways of deploying models that can be used for real time application needs using REST.  For basic prototypes, MLflow can act as development deployment server.  The MLflow implementation is backed by the Python library Flask.  *This is not intended to for production environments.*
# MAGIC 
# MAGIC For production RESTful deployment, there are two general options: a managed solution or a custom solution.  The two most popular managed solutions are Amazon's SageMaker and Azure's AzureML.  Custom solutionss often involve deployments using a range of tools, but often using Docker, Kubernetes, and Elastic Beanstalk.
# MAGIC 
# MAGIC One of the crucial elements of deployment in containerization, where software is packaged and isolated with its own application, tools, and libraries.  Containers are a more lightweight alternatie to virtual machines.
# MAGIC 
# MAGIC Finally, embedded solutions are another way of deploying machine learning models, such as storing a model on IoT devices for inference.
# MAGIC 
# MAGIC -----------------------
# MAGIC 
# MAGIC TODO: incorporate this material:
# MAGIC * MLeap is an option too
# MAGIC * For MLeap, See [Adam's talk](https://www.youtube.com/watch?v=KOehXxEgXFM) and [his notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/7033591513855707/699028214697099/6890845802385168/latest.html) 
# MAGIC * Look at Azure ML example: https://trainers.cloud.databricks.com/#notebook/7512830/command/7512855

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Prototyping with MLflow

# COMMAND ----------

# MAGIC %md
# MAGIC https://www.mlflow.org/docs/latest/models.html#pyfunc-deployment

# COMMAND ----------

import mlflow
import mlflow.pyfunc

class TestModel(mlflow.pyfunc.PythonModel):
  
  def predict(self, context, input_df):
    return 5
  
artifact_path="pyfunc-model"
with mlflow.start_run():
  mlflow.pyfunc.log_model(artifact_path=artifact_path, python_model=TestModel())
  run_id = mlflow.active_run().info.run_uuid
  print(run_id)

# COMMAND ----------

from multiprocessing import Process

server_port_number = 6501

def run_server():
  try:
    import mlflow.pyfunc.cli
    from click.testing import CliRunner
    CliRunner().invoke(mlflow.pyfunc.cli.commands, ['serve', "--model-path", artifact_path, "--run-id", run_id, "--port", server_port_number, "--host", "127.0.0.1", "--no-conda"])
  except Exception as e:
    print(e)

p = Process(target=run_server) # Run as a background process
p.start()

# COMMAND ----------

import json
import requests
import pandas as pd
input_df = pd.DataFrame([0])
input_json = input_df.to_json(orient='split')

headers = {'Content-type': 'application/json'}

response = requests.post(url="http://localhost:{port_number}/invocations".format(port_number=server_port_number), headers=headers, data=input_json)
print(response)
print(response.text)

# COMMAND ----------

input_json

# COMMAND ----------

# MAGIC %md
# MAGIC You can do the same in bash.

# COMMAND ----------

# MAGIC %sh (echo -n '{"columns":[0],"index":[0],"data":[[0]]}') | curl -H "Content-Type: application/json" -d @- http://127.0.0.1:6501/invocations

# COMMAND ----------

# MAGIC %sh (echo -n "[[3.0, 0.0, 0.0, 37.77231548637585, -122.43612044376671, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 3.0, 9.0, 84.0, 9.0, 9.0, 10.0, 10.0, 9.0, 9.0]]") | curl -H "Content-Type: application/json" -d @- http://127.0.0.1:5000/invocations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed Service Walkthrough
# MAGIC 
# MAGIC [Check out this notebook.]($./Extras/SageMaker-Deployment )
# MAGIC 
# MAGIC * [AWS](https://www.mlflow.org/docs/latest/models.html#deploy-a-python-function-model-on-amazon-sagemaker)
# MAGIC * [Azure](https://www.mlflow.org/docs/latest/models.html#deploy-a-python-function-model-on-microsoft-azure-ml)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SageMaker
# MAGIC 
# MAGIC This example assumes that the model was already deployed to SageMaker.  See the walkthrough above in case you missed it.  Now let's look at how we'll query that REST endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC First set AWS keys as environment variables.  **This is not a best practice since this is not the most secure way of handling credentials.**  This works in our case sense the keys have a very limited policy associated with them.

# COMMAND ----------

import os

# Set AWS credentials as environment variables
os.environ["AWS_ACCESS_KEY_ID"] = 'AKIAI4T2MLVBUB372FAA'
os.environ["AWS_SECRET_ACCESS_KEY"] = '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' # READ ONLY ACCESS KEYS
os.environ["AWS_DEFAULT_REGION"] = 'us-west-2'

# COMMAND ----------

# MAGIC %md
# MAGIC Use `boto3`, the library for interacting with AWS in Python, to check the application status.

# COMMAND ----------

import boto3

def check_status(appName):
  sage_client = boto3.client('sagemaker', region_name="us-west-2")
  endpoint_description = sage_client.describe_endpoint(EndpointName=appName)
  endpoint_status = endpoint_description["EndpointStatus"]
  return endpoint_status

print("Application status is: {}".format(check_status(appName)))

# COMMAND ----------

# MAGIC %md
# MAGIC Import the Airbnb dataset and pull out the first record.

# COMMAND ----------

import pandas as pd
import random
from sklearn.model_selection import train_test_split

df = pd.read_csv("/dbfs/mnt/conor-work/airbnb/airbnb-cleaned-mlflow.csv")
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)
query_input = X_train.iloc[[0]].values.flatten().tolist()

print("Using input vector: {}".format(query_input))

# COMMAND ----------

# MAGIC %md
# MAGIC Define a helper function that connects to the `sagemaker-runtime` client and sends the record in the appropriate JSON format.

# COMMAND ----------

import json

def query_endpoint_example(inputs, appName="airbnb-latest-0001", verbose=True):
  if verbose:
    print("Sending batch prediction request with inputs: {}".format(inputs))
  client = boto3.session.Session().client("sagemaker-runtime", "us-west-2")
  
  response = client.invoke_endpoint(
      EndpointName=appName,
      Body=json.dumps(inputs),
      ContentType='application/json',
  )
  preds = response['Body'].read().decode("ascii")
  preds = json.loads(preds)
  
  if verbose:
    print("Received response: {}".format(preds))
  return preds

# COMMAND ----------

# MAGIC %md
# MAGIC Query the endpoint.

# COMMAND ----------

prediction = query_endpoint_example(inputs=[query_input])

# COMMAND ----------

# MAGIC %md
# MAGIC Now try the same but by using more than just one record.  Create a helper function to query the endpoint with a number of random samples.

# COMMAND ----------

def random_n_samples(n, df=X_train, verbose=False):
  dfShape = X_train.shape[0]
  samples = []
  
  for i in range(n):
    sample = X_train.iloc[[random.randint(0, dfShape-1)]].values
    samples.append(sample.flatten().tolist())
  
  return query_endpoint_example(samples, appName="airbnb-latest-0001", verbose=verbose)

# COMMAND ----------

# MAGIC %md
# MAGIC Test this using 10 samples.  The payload for SageMaker can be 1 or more samples.

# COMMAND ----------

random_n_samples(10, verbose=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Compare the times between payload sizes.  **Notice how sending more records at a time reduces the time to prediction for each individual record.**

# COMMAND ----------

# MAGIC %timeit -n5 random_n_samples(100)

# COMMAND ----------

# MAGIC %timeit -n5 random_n_samples(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** Why is exploratory analysis important?  
# MAGIC **Answer:** The main goal of exploratory analysis is to build intuition from the dataset that will determine future modeling decisions.  This is often an overlooked step.  Having a limited understanding of the data leads to downstream problems like unexpected outputs and errors as well as under-performing models.
# MAGIC 
# MAGIC **Question:** What summary statistics are helpful?
# MAGIC **Answer:** The most helpful statistics quantify the center and spread of the data (usually through the mean and standard deviation).  Counts (which reflect missing values) as well as the minimum and maximum are also informative.  A median can be used instead of a mean as well since a median is less influenced by outliers.
# MAGIC 
# MAGIC **Question:** What are the most informative plots and how can I do that on Databricks?
# MAGIC **Answer:** For a single variable, histograms and KDE plots are quite informative.  Box plots or violin plots are helpful as well.  Each shows the distribution of data and outliers in different ways.  When plotting multiple variables, scatterplots and bar charts can be helpful.  Scattermatrixes represent multiple features well.  This can be accomplished on Databricks either by using the built-in plotting functionality or by using the `display()` function to display plots generated with Python libraries like Matplotlib and Seaborn.
# MAGIC 
# MAGIC **Question:** What is correlation?
# MAGIC **Answer:** Correlation looks at the linear relationship between two variables.  A positive correlation means that one variable increases linearly relative to another.  A negative correlation means that one variable increases as the other decreases.  No correlation means that the two variables are generally independent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Monitoring]($./09-Monitoring ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on general mathematical functions in Spark?
# MAGIC **A:** Check out the Databricks blog post <a href="https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html" target="_blank">Statistical and Mathematical Functions with DataFrames in Spark</a>
# MAGIC 
# MAGIC **Q:** Where can I find out more information on the machine learning Spark library and statistics?
# MAGIC **A:** Check out <a href="https://spark.apache.org/docs/latest/ml-statistics.html" target="_blank">the Spark documentation</a>
# MAGIC 
# MAGIC **Q:** Can I do kernel density estimation in Spark?
# MAGIC **A:** Yes.  See the <a href="https://spark.apache.org/docs/latest/mllib-statistics.html#kernel-density-estimation" target="_blank">Spark documentation for details</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>