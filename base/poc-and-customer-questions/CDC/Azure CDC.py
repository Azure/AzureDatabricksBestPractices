# Databricks notebook source
storage_account_name = "joelsimpleblobstore"
storage_account_access_key = "XuKm58WHURoiBqbxGuKFvLDQwDu4ctbjNgphY14qhRsUmsiFMdBmw2UbIIBYQvawsydVSiqZVaRPaWv/v2l/7g=="

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %fs ls wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/clicks

# COMMAND ----------

from pyspark.sql.types  import *
click_schema = (StructType([
  StructField("exchangeID",IntegerType(),True),
  StructField("publisher",StringType(),True),
  StructField("creativeID",IntegerType(),True),
  StructField("click",StringType(),True),
  StructField("advertiserID",IntegerType(),True),
  StructField("uid",StringType(),True),
  StructField("browser",StringType(),True),
  StructField("geo",StringType(),True),
  StructField("bidAmount",DoubleType(),True),
  StructField("impTimestamp",TimestampType(),True),
  StructField("date",DateType(),True)])
)

# COMMAND ----------

people_profiles_schema = (StructType([
  StructField("uid",StringType(),True),
  StructField("street",StringType(),True),
  StructField("city",StringType(),True),
  StructField("state",StringType(),True),
  StructField("zipcode",StringType(),True),
  StructField("firstname",StringType(),True),
  StructField("lastname",StringType(),True)])
)

# COMMAND ----------

click = spark.readStream.schema(click_schema).option("maxFilesPerTrigger", 1).parquet("wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/clicks")

# COMMAND ----------

display(click)

# COMMAND ----------

people_profile = spark.readStream.schema(people_profiles_schema).option("maxFilesPerTrigger", 1).parquet("wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/people_profiles")

# COMMAND ----------

display(people_profile)

# COMMAND ----------

