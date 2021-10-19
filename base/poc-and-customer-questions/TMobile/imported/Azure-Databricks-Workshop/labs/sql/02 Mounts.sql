-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Challenges
-- MAGIC * Data is all over the place, so reports, KPIs and DS is hard on exiting tools
-- MAGIC * Azure Storage or Azure Data Lake - Is a place to store all data, big and small
-- MAGIC * Access and Security is harder on these sources
-- MAGIC 
-- MAGIC ### Azure Databricks Solutions
-- MAGIC * Easily and securely access these data stores, mount points

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Getting Started with Azure Storage and Azure Data Lake 
-- MAGIC 
-- MAGIC **Databricks Mount Points:**
-- MAGIC - Connect to our Azure Storage Account - https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-storage.html
-- MAGIC - Connect to our Azure Data Lake - https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Connect to our Azure Storage Account
-- MAGIC 
-- MAGIC Next, let's connect to the read-only Blob store you'll have access to for data needed in this course.  We can easily mount data in blob stores to Azure Databricks for fast and scalable data storage
-- MAGIC 
-- MAGIC *Note:* You will have to have a cluster running to execute this code

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <h2 style="color:green">Databricks Tip</h2>
-- MAGIC 
-- MAGIC * Mount points to hide keys
-- MAGIC * Rich integration with Databricks, Spark and ADL, Storage

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2 style="color:red">IMPORTANT!</h2> This notebook must be run using Azure Databricks runtime 4.0 or better.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.help()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC # We have already mounted this directory, so no need to do it again. 
-- MAGIC 
-- MAGIC # This is what the code looks like:
-- MAGIC 
-- MAGIC # dbutils.fs.mount(
-- MAGIC #   source = "wasbs://source@adbworkshops.blob.core.windows.net/",
-- MAGIC #   mount_point = "/mnt/training-sources/",
-- MAGIC #   extra_configs = {"fs.azure.sas.source.adbworkshops.blob.core.windows.net": "SAS-KEY"})

-- COMMAND ----------

-- MAGIC %fs ls /mnt/training-sources/