# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 6: Batch

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Load in same Airbnb data and create train/test split.

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

# COMMAND ----------



# COMMAND ----------

PrimaryRead = "SidFEUCE2RA5qMYdUonLdwp4bAYW6Xj4R5Xdw4Bo7F4fkG9anp7IhjwEZc9wOvM4FJBU84efcupWZTrabFlinA==" # Read only keys
Endpoint = "https://airbnbpredictions.documents.azure.com:443/"
CosmosDatabase =  "predictions"
CosmosCollection = "predictions"

if not PrimaryRead:
  raise Exception("Don't forget to specify the cosmos keys in this cell.")

cosmosConfig = {
  "Endpoint": Endpoint,
  "Masterkey": PrimaryRead,
  "Database": CosmosDatabase,
  "Collection": CosmosCollection
}

# COMMAND ----------

from pyspark.sql.functions import col

def predict(id):
  prediction = (spark.read
    .format("com.microsoft.azure.cosmosdb.spark")
    .options(**cosmosConfig)
    .load()
    .filter(col("id") == id)
    .select("prediction")
    .first()
   )
  return prediction[0]

predict("7b22b1d3-c634-4bad-a854-17e0669aa685")

# COMMAND ----------

# MAGIC %timeit predict("7b22b1d3-c634-4bad-a854-17e0669aa685")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>