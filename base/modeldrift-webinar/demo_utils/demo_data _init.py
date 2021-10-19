# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../data/data_access

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear pre-data for ru-runs

# COMMAND ----------

def clear_for_demo():
  dbutils.fs.rm(sensor_reading_blob, True)
  dbutils.fs.rm(product_quality_blob, True)
  dbutils.fs.rm(predicted_quality_blob, True)
  dbutils.fs.rm(predicted_quality_cp_blob, True)
  return True

# COMMAND ----------

## Run this to clear predicted quality tables, in case you want to try again
clear_for_demo()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate data for demo

# COMMAND ----------

df = spark.range(1,8000)
# Setup Temperature, Pressure, Duration
df = df.select("id", F.rand(seed=10).alias("temp_raw"), F.randn(seed=27).alias("pressure_raw"), F.rand(seed=45).alias("duration_raw"), 
               F.randn(seed=54).alias("temp_n"), F.randn(seed=78).alias("pressure_n"), F.randn(seed=96).alias("duration_n"), F.round(F.rand()*7.5*60,0).alias("timestamp_n"))
df = df.withColumn('pid', (100000 + df["id"]))
df = (df.withColumn("temp_raw", (10.0*df["temp_raw"])+350)
      .withColumn("pressure_raw", (2.0*df["pressure_raw"])+12)
      .withColumn("duration_raw", (4.0*df["duration_raw"])+28.5)
      .withColumn("timestamp", ((df["id"]*7.5*60)+1561939200+df["timestamp_n"]).cast('timestamp'))
     )
df = df.withColumn("process_time", df["timestamp"])
df = df.withColumn("qualitycheck_time", F.date_trunc("day", F.date_add(df["timestamp"], 2)))

# Assign good vs bad for quality
df = df.withColumn(
    'quality',
    F.when(F.col('id').between(1,3400) & F.col('temp_raw').between(351,359) & F.col('pressure_raw').between(8,15) & F.col('duration_raw').between(28,32), 1)\
    .when(F.col('id').between(3401,6500) & F.col('temp_raw').between(354,359) & F.col('pressure_raw').between(8,15) & F.col('duration_raw').between(28,32), 1)\
    .when(F.col('id').between(6501,8000) & F.col('temp_raw').between(354,359) & F.col('pressure_raw').between(12,15) & F.col('duration_raw').between(28,32), 1)\
    .otherwise(0)
)
# Add some noise
df = (df.withColumn("temp", df["temp_raw"]+(dg_noise["temp_noise"]*df["temp_n"]))
      .withColumn("pressure", df["pressure_raw"]+(dg_noise["pressure_noise"]*df["pressure_n"]))
      .withColumn("duration", df["duration_raw"]+(dg_noise["duration_noise"]*df["duration_n"]))
     )
df = df.select('pid','timestamp', 'process_time', 'qualitycheck_time', 'temp_raw', 'temp', 'pressure_raw', 'pressure', 'duration_raw', 'duration', 'quality')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Send data for range: 2019-07-01 00:00:00 - 2019-07-16 00:00:00

# COMMAND ----------

# First Model generation
df1 = df.filter(df.timestamp<=F.unix_timestamp(F.lit('2019-07-16 00:00:00')).cast('timestamp'))

# COMMAND ----------

df1.select('pid', 'process_time', 'temp', 'pressure', 'duration').write.format("delta").mode("overwrite").option("maxRecordsPerFile", 50).save(sensor_reading_blob)
df1.select('pid', 'qualitycheck_time', 'quality').write.format("delta").mode("overwrite").option("maxRecordsPerFile", 50).save(product_quality_blob)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append data for range: 2019-07-16 00:00:00 - 2019-07-21 00:00:00

# COMMAND ----------

# Data for drift and 2nd model generation
df2 = df.filter( (df.timestamp>=F.unix_timestamp(F.lit('2019-07-16 00:00:00')).cast('timestamp')) & (df.timestamp<=F.unix_timestamp(F.lit('2019-07-21 00:00:00')).cast('timestamp')))

# COMMAND ----------

df2.select('pid', 'process_time', 'temp', 'pressure', 'duration').write.format("delta").mode("append").option("maxRecordsPerFile", 50).save(sensor_reading_blob)
df2.select('pid', 'qualitycheck_time', 'quality').write.format("delta").mode("append").option("maxRecordsPerFile", 50).save(product_quality_blob)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Send data for range: > 2019-07-21 00:00:00 

# COMMAND ----------

# Data after 2nd model deployment
df3 = df.filter(df.timestamp>F.unix_timestamp(F.lit('2019-07-21 00:00:00')).cast('timestamp'))

# COMMAND ----------

df3.select('pid', 'process_time', 'temp', 'pressure', 'duration').write.format("delta").mode("append").option("maxRecordsPerFile", 50).save(sensor_reading_blob)
df3.select('pid', 'qualitycheck_time', 'quality').write.format("delta").mode("append").option("maxRecordsPerFile", 50).save(product_quality_blob)

# COMMAND ----------

