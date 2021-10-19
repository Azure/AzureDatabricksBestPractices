# Databricks notebook source
dbutils.fs.rm("/mnt/databricks-paul/sfdc-delta", True)
dbutils.fs.rm("/mnt/databricks-paul/sfdc-delta-checkpoints", True)

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-paul/update-checkpoints", True)

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-paul/sfdc-delta-checkpoints", True)

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-paul/sfdc-delta-old", True)
dbutils.fs.mv("/mnt/databricks-paul/sfdc-delta-checkpoints-old",  True)