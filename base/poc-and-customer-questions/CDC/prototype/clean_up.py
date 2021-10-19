# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS paul.click_with_profile;
# MAGIC DROP TABLE IF EXISTS paul.profile_data;
# MAGIC DROP TABLE IF EXISTS paul.signal_store;

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-paul/profile-data", True)
dbutils.fs.rm("/mnt/databricks-paul/click_with_profile", True)
dbutils.fs.rm("/mnt/databricks-paul/click_with_profile-checkpoints", True)
dbutils.fs.rm("/mnt/databricks-paul/signal_store", True)
dbutils.fs.rm("/mnt/databricks-paul/signal_store-checkpoints", True)
dbutils.fs.rm("/mnt/databricks-paul/cdc_transformed-checkpoints", True)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###[Back](https://demo.cloud.databricks.com/#notebook/2504942/command/2504948)