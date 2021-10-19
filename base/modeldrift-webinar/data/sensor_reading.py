# Databricks notebook source
from pyspark.sql.types  import *

# COMMAND ----------

def get_sensor_reading():
  sensor_reading_schema = (StructType([
    StructField("pid",LongType(),True),
    StructField("process_time",TimestampType(),True),
    StructField("temp",DoubleType(),True),
    StructField("pressure",DoubleType(),True),
    StructField("duration",DoubleType(),True)])
  )
  return spark.read.format("delta").schema(sensor_reading_schema).load(sensor_reading_blob)

# COMMAND ----------

def stream_sensor_reading():
  return spark.readStream.format("delta").load(sensor_reading_blob)