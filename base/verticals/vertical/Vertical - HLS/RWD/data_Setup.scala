// Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.wesdias.blob.core.windows.net",
  "tXpfmnGyDhS6SCSZTXmBmzMIDQTtKdeS2XsCgpj8dOTU5TrrciC3p5J+rIsf6zkVrcV1nXKPbZifNbByCWFEXg==")

// COMMAND ----------

val open_payments_companies = spark.read.format("parquet")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/medicare/payment/open_payments_companies/")

open_payments_companies.write.saveAsTable("open_payments_companies")

// COMMAND ----------

val open_payments_edges = spark.read.format("parquet")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/medicare/payment/open_payments_edges/")

open_payments_edges.write.saveAsTable("open_payments_edges")

// COMMAND ----------

val open_payments_physicians = spark.read.format("parquet")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/medicare/payment/open_payments_physicians/")

open_payments_physicians.write.saveAsTable("open_payments_physicians")