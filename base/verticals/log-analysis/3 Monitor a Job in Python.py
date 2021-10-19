# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # **Monitoring in Python**
# MAGIC This notebook describes how to monitor runs of the log analysis notebook. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 1:** Create a table that will contain stats about the log analysis run that can be monitored.
# MAGIC Optionally, these commands could be moved into the logs analysis notebook themselves if there are parameters there to track - such as the number of errored inputs, etc.

# COMMAND ----------

# MAGIC %sql CREATE TABLE IF NOT EXISTS logsAnalysisPython (runStart INT, runDuration INT)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 2:** Run the logs analysis notebook and time it.
# MAGIC Note: Python notebooks can be run from other Python notebooks only.

# COMMAND ----------

import time
runStart = time.time()

# COMMAND ----------

runStart

# COMMAND ----------

# MAGIC %run "/field_eng/databricks_guide/13 Demos/1 Log Analysis/2 Log Analysis in Python"

# COMMAND ----------

# MAGIC %fs ls /mnt/

# COMMAND ----------

runEnd = time.time()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 3:** Insert a row to indicate the result of this run.

# COMMAND ----------

from pyspark.sql import Row
rdd = sc.parallelize([Row(runStart=runStart, runDuration=(runEnd - runStart))])
schemaRDD = sqlContext.createDataFrame(rdd)
schemaRDD.registerTempTable("oneRun")

# COMMAND ----------

# MAGIC %sql insert into TABLE logsAnalysisPython select runStart, runDuration from oneRun limit 1

# COMMAND ----------

# MAGIC %sql select from_unixtime(runstart) as starttime, runduration from logsAnalysisPython