# Databricks notebook source
from pyspark.sql.types  import *

# COMMAND ----------

def get_predicted_quality():
  predicted_quality_schema = (StructType([
    StructField("pid",LongType(),True),
    StructField("process_time",TimestampType(),True),
    StructField("predicted_quality",IntegerType(),True)])
  )
  return spark.read.format("delta").load(predicted_quality_blob)

# COMMAND ----------

def stream_predicted_quality():
  return spark.readStream.format("delta").load(predicted_quality_blob)