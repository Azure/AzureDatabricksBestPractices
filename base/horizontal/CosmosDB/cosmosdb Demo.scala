// Databricks notebook source
import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._

// COMMAND ----------

// Configure the connection to your collection in Cosmos DB.
// Please refer to https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references
// for the description of the available configurations.
val configMap = Map(
  "Endpoint" -> {URI of the Azure Cosmos DB account},
  "Masterkey" -> {Key used to access the account},
  "Database" -> {Database name},
  "Collection" -> {Collection name},
  "preferredRegions" -> {Preferred regions})
val config = Config(configMap)

// COMMAND ----------

// Generate a simple dataset containing five values and
// write the dataset to Cosmos DB.
val df = spark.range(5).select(col("id").cast("string").as("value"))
CosmosDBSpark.save(df, config)

// COMMAND ----------

// Read the data written by the previous cell back.
val dataInCosmosDb = spark.sqlContext.read.cosmosDB(config)
display(dataInCosmosDb.orderBy(col("value")))