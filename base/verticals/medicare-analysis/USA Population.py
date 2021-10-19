# Databricks notebook source
df = sqlContext.read.\
      format("com.databricks.spark.csv")\
      .options(header='true', inferschema='true', delimiter='|')\
      .load("/mnt/mwc/cms/population")
      
df.createOrReplaceTempView("usa_population")
#display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE TABLE usa_population