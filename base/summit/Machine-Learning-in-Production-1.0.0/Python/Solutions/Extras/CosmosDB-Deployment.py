# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Cosmos DB Deployment
# MAGIC 
# MAGIC Cosmos DB is a Microsoft Azure technology for low latency, high availability, and globally distributed data.  This lesson introduces connecting to Cosmos DB as a way of serving Machine learning models.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC * Create a Cosmos DB database and collection
# MAGIC * Write to a Cosmos DB collection
# MAGIC * Confirm that connection is functioning
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Some of the details of configuring Azure are left up to the user and will depend on your specific Azure configuration.  **This notebook cannot be run as-is.**  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Working with Cosmos DB
# MAGIC 
# MAGIC Cosmos DB is Microsoft Azure's document databases.  It's low latency and high availability.  One of the core benefits of this technology is that you can globally replicate your data easily, always keeping your data up to date across the globe.
# MAGIC 
# MAGIC This lesson requires permissions to create resources on Azure.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You will need to install the Azure connector from the Maven coordinates `com.microsoft.azure:azure-cosmosdb-spark_2.4.0_2.11:1.3.5`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Collection
# MAGIC 
# MAGIC First, access the Azure Portal via the link in Azure Databricks
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/airlift/AAHy9d889ERM6rJd2US1kRiqGCLiHzgmtFsB.png" width=800px />

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC Choose **Azure Cosmos DB**<br><br>
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/cosmos1.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC Choose **Add**<br><br>
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/cosmos2.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC Insert the information seen below.  Choose your own subscription and resource group.<br><br>
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/cosmos3.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Create a new collection for your predictions using the information below.<br><br>
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/cosmos5.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC Copy the key and URI from the keys tab.<br><br>
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/cosmos4.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC Fill out the following variables with your information.

# COMMAND ----------

# Fill out the following variables with your information

PrimaryReadWriteKey = "" # FILL IN
Endpoint = "" # FILL IN
CosmosDatabase =  "predictions"
CosmosCollection = "predictions"

if not PrimaryReadWriteKey:
  raise Exception("Don't forget to specify the cosmos keys in this cell.")

cosmosConfig = {
  "Endpoint": Endpoint,
  "Masterkey": PrimaryReadWriteKey,
  "Database": CosmosDatabase,
  "Collection": CosmosCollection
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to the Connection
# MAGIC 
# MAGIC Now that the database and collection have been made, write to the collection

# COMMAND ----------

# MAGIC %md
# MAGIC Configure your environment.

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Train a basic random forest model.

# COMMAND ----------

import mlflow
from mlflow.exceptions import MlflowException
from  mlflow.tracking import MlflowClient
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
import mlflow.sklearn

# Define paths
experimentPath = "/Users/" + username + "/CosmosDB-Deployment"
modelPath = "random-forest-model"

# Create experiment
try:
  experimentID = mlflow.create_experiment(experimentPath)
except MlflowException:
  experimentID = MlflowClient().get_experiment_by_name(experimentPath).experiment_id
  mlflow.set_experiment(experimentPath)

print("The experiment can be found at the path `{}` and has an experiment_id of `{}`".format(experimentPath, experimentID))

# Train and log model
df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

rf = RandomForestRegressor(n_estimators=1000, max_depth=20)
rf.fit(X_train, y_train)

with mlflow.start_run(experiment_id=experimentID, run_name="RF Model") as run: 
  mlflow.sklearn.log_model(rf, modelPath)
  runID = run.info.run_uuid
  
print("Run completed with ID {} and path {}".format(runID, modelPath))

# COMMAND ----------

# MAGIC %md
# MAGIC Convert the `sklearn` model to a Spark UDF.

# COMMAND ----------

pyfunc_udf = mlflow.pyfunc.spark_udf(spark, "random-forest-model", run_id=runID)

# COMMAND ----------

# MAGIC %md
# MAGIC Read in the same data as a Spark DataFrame.

# COMMAND ----------

sparkDF = (spark.read
  .option("HEADER", True)
  .csv("/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")
  .drop("price")
)

display(sparkDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Apply the model to the airbnb data.

# COMMAND ----------

predictionsDF = sparkDF.withColumn("prediction", pyfunc_udf(*sparkDF.columns))

display(predictionsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the data to Cosmos.

# COMMAND ----------

(predictionsDF.write
  .mode("overwrite")
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**cosmosConfig)
  .save())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confirm the Connection is Working
# MAGIC 
# MAGIC Confirm the write by reading the data back from Cosmos.

# COMMAND ----------

dfCosmos = (spark.read
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**cosmosConfig)
  .load())

display(dfCosmos)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>