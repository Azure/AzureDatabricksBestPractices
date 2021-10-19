# Databricks notebook source
from pyspark.sql.types  import *

# COMMAND ----------

def get_product_quality():
  product_quality_schema = (StructType([
    StructField("pid",LongType(),True),
    StructField("qualitycheck_time",TimestampType(),True),
    StructField("quality",IntegerType(),True)])
  )
  return spark.read.format("delta").schema(product_quality_schema).load(product_quality_blob)

# COMMAND ----------

def stream_product_quality():
  return spark.readStream.format("delta").load(product_quality_blob)