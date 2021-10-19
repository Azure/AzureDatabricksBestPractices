// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Writing Data
// MAGIC 
// MAGIC Just as there are many ways to read data, we have just as many ways to write data.
// MAGIC 
// MAGIC In this notebook, we will take a quick peek at how to write data back out to Parquet files.
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC - Writing data to Parquet files

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Writing Data
// MAGIC 
// MAGIC Let's start with one of our original CSV data sources, **pageviews_by_second.tsv**:

// COMMAND ----------

import org.apache.spark.sql.types._

val csvSchema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val csvFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

val csvDF = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(csvSchema)
  .csv(csvFile)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have a `DataFrame`, we can write it back out as Parquet files or other various formats.

// COMMAND ----------

val fileName = userhome + "/pageviews_by_second.parquet"

csvDF.write                        // Our DataFrameWriter
  .option("compression", "snappy") // One of none, snappy, gzip, and lzo
  .mode("overwrite")               // Replace existing files
  .parquet(fileName)               // Write DataFrame to Parquet files

// COMMAND ----------

// MAGIC %md
// MAGIC Now that the file has been written out, we can see it in the DBFS:

// COMMAND ----------

display(
  dbutils.fs.ls(fileName)
)

// COMMAND ----------

// MAGIC %md
// MAGIC And lastly we can read that same parquet file back in and display the results:

// COMMAND ----------

display(
  spark.read.parquet(fileName)
)


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>