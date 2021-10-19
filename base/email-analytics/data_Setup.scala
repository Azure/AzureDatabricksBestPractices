// Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.wesdias.blob.core.windows.net",
  "tXpfmnGyDhS6SCSZTXmBmzMIDQTtKdeS2XsCgpj8dOTU5TrrciC3p5J+rIsf6zkVrcV1nXKPbZifNbByCWFEXg==")

// COMMAND ----------

// MAGIC %fs cp wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/security/email/mail-2015.avro /mnt/azure/dataset/security/email/mail-2015.avro