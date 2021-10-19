// Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.wesdias.blob.core.windows.net",
  "tXpfmnGyDhS6SCSZTXmBmzMIDQTtKdeS2XsCgpj8dOTU5TrrciC3p5J+rIsf6zkVrcV1nXKPbZifNbByCWFEXg==")

// COMMAND ----------

// MAGIC %fs cp wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/medicare/recomendation/HealthPlan.xml /mnt/azure/medicare/recomendation/HealthPlan.xml

// COMMAND ----------

// MAGIC %fs cp wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/medicare/recomendation/quote2.xml /mnt/azure/medicare/recomendation/quote2.xml