// Databricks notebook source
// MAGIC %md #Serve Model (Batch Mode)

// COMMAND ----------

// DBTITLE 1,Load Persisted ML Pipeline
import org.apache.spark.ml.PipelineModel
val model = PipelineModel.load("/mnt/joewiden/amazonRevML")

// COMMAND ----------

// MAGIC %fs 
// MAGIC ls /mnt/joewiden/amazonRevML

// COMMAND ----------

// DBTITLE 1,Score Recent Data (last day)
val newData = spark.sql("select * from amazon where time > ((select max(time) from amazon) - 86400)")
newData.cache.count

// COMMAND ----------

// DBTITLE 1,Score New Data
val scoredData = model.transform(newData)

// COMMAND ----------

display(scoredData.select("label", "prediction", "probability", "review"))

// COMMAND ----------

