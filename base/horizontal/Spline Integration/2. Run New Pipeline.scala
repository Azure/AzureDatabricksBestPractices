// Databricks notebook source
import za.co.absa.spline.core.SparkLineageInitializer._
import za.co.absa.spline.core.conf.DefaultSplineConfigurer
import za.co.absa.spline.persistence.mongo.MongoPersistenceFactory
import org.apache.commons.configuration.BaseConfiguration
val config = new BaseConfiguration()
config.setProperty("spline.persistence.factory", classOf[za.co.absa.spline.persistence.mongo.MongoPersistenceFactory].getName)
config.setProperty("spline.mongodb.url", "mongodb://localhost")
config.setProperty("spline.mongodb.name", "my_lineage_database_name")


// COMMAND ----------

val splineConfig = new DefaultSplineConfigurer(config)

import org.apache.spark.sql.{SaveMode}

spark.enableLineageTracking(splineConfig)



// COMMAND ----------

val sourceDS = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/data.csv").as("source").filter($"total_response_size" > 1000).filter($"count_views" > 10)

val domainMappingDS = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/domain.csv").as("mapping")

val joinedDS = sourceDS.join(domainMappingDS, $"domain_code" === $"d_code", "left_outer").select($"page_title".as("page"), $"d_name".as("domain"), $"count_views")


// COMMAND ----------

joinedDS.write.mode(SaveMode.Overwrite).parquet("file:////tmp/wesley/test2/results/job1_results")