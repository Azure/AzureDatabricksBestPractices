// Databricks notebook source
// MAGIC %scala
// MAGIC spark.conf.set(
// MAGIC   "fs.azure.account.key.wesdias.blob.core.windows.net",
// MAGIC   "tXpfmnGyDhS6SCSZTXmBmzMIDQTtKdeS2XsCgpj8dOTU5TrrciC3p5J+rIsf6zkVrcV1nXKPbZifNbByCWFEXg==")

// COMMAND ----------

// MAGIC %sql create database fraud

// COMMAND ----------

val atm_customers = spark.read.format("parquet")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/Fintech/fightatmfraud/atm_customers/")

atm_customers.write.saveAsTable("fraud.atm_customers")

// COMMAND ----------

val atm_locations = spark.read.format("parquet")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/Fintech/fightatmfraud/atm_locations/")

atm_locations.write.saveAsTable("fraud.atm_locations")

// COMMAND ----------

val atm_visits = spark.read.format("parquet")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/Fintech/fightatmfraud/atm_visits/")

atm_visits.write.saveAsTable("fraud.atm_visits")