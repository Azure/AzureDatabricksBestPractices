// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ###Setup
// MAGIC 
// MAGIC To set up Azure Cosmos DB Spark connector:
// MAGIC 
// MAGIC Download the following libraries in the form of jar files, following the links. Please note that you do not need to download dependencies of these libraries.
// MAGIC 
// MAGIC com.microsoft.azure:azure-cosmosdb-spark_2.2.0_2.10:0.0.4 or com.microsoft.azure:azure-cosmosdb-spark_2.2.0_2.11:0.0.4. Please use the jar with the Scala version corresponding to the Scala version of your Runtime cluster.
// MAGIC 
// MAGIC com.microsoft.azure:azure-documentdb:1.13.0
// MAGIC 
// MAGIC com.microsoft.azure:azure-documentdb-rx:0.9.0-rc2
// MAGIC 
// MAGIC io.reactivex:rxjava:1.3.0
// MAGIC 
// MAGIC io.reactivex:rxnetty:0.4.20
// MAGIC org.json:json:20140107
// MAGIC 
// MAGIC Upload the downloaded jar files to Databricks following the guidance in Uploading Libraries.
// MAGIC 
// MAGIC Attach the uploaded libraries to your cluster.