// Databricks notebook source
// MAGIC %python
// MAGIC spark.conf.set(
// MAGIC   "fs.azure.account.key.wesdias.blob.core.windows.net",
// MAGIC   "tXpfmnGyDhS6SCSZTXmBmzMIDQTtKdeS2XsCgpj8dOTU5TrrciC3p5J+rIsf6zkVrcV1nXKPbZifNbByCWFEXg==")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC insurance_claims = sqlContext.read.format('csv').options(header='true', inferSchema='true').load("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/Fintech/insuranceclaims/insurance_claims2.csv")