// Databricks notebook source
// MAGIC %python
// MAGIC spark.conf.set(
// MAGIC   "fs.azure.account.key.wesdias.blob.core.windows.net",
// MAGIC   "tXpfmnGyDhS6SCSZTXmBmzMIDQTtKdeS2XsCgpj8dOTU5TrrciC3p5J+rIsf6zkVrcV1nXKPbZifNbByCWFEXg==")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC menu = spark.read.parquet('wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/Fintech/demandforecast/menu')
// MAGIC menu.createOrReplaceTempView("menu")
// MAGIC 
// MAGIC transactions = spark.read.parquet('wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/Fintech/demandforecast/transactions_final')
// MAGIC transactions.createOrReplaceTempView("transactions")
// MAGIC 
// MAGIC locations = spark.read.parquet('wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/Fintech/demandforecast/geolocations')
// MAGIC locations.createOrReplaceTempView("locations")
// MAGIC 
// MAGIC sales = spark.read.parquet('wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/Fintech/demandforecast/sales_final')
// MAGIC sales.createOrReplaceTempView("sales")