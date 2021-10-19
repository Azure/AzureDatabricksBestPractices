# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %run ./sensor_reading

# COMMAND ----------

# MAGIC %run ./product_quality

# COMMAND ----------

# MAGIC %run ./predicted_quality