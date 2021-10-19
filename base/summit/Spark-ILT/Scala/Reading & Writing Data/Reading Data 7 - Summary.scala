// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading Data - Summary
// MAGIC 
// MAGIC In this notebook, we will quickly compare the various methods for reading in data.
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC - Contrast the various techniques for reading data.

// COMMAND ----------

// MAGIC %md
// MAGIC ## General
// MAGIC - `SparkSession` is our entry point for working with the `DataFrames` API
// MAGIC - `DataFrameReader` is the interface to the various read operations
// MAGIC - Each reader behaves differently when it comes to the number of initial partitions and depends on both the file format (CSV vs Parquet vs ORC) and the source (Azure Blob vs Amazon S3 vs JDBC vs HDFS)
// MAGIC - Ultimately, it is dependent on the implementation of the `DataFrameReader`

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ## Comparison
// MAGIC | Type    | <span style="white-space:nowrap">Inference Type</span> | <span style="white-space:nowrap">Inference Speed</span> | Reason                                          | <span style="white-space:nowrap">Should Supply Schema?</span> |
// MAGIC |---------|--------------------------------------------------------|---------------------------------------------------------|----------------------------------------------------|:--------------:|
// MAGIC | <b>CSV</b>     | <span style="white-space:nowrap">Full-Data-Read</span> | <span style="white-space:nowrap">Slow</span>            | <span style="white-space:nowrap">File size</span>  | Yes            |
// MAGIC | <b>Parquet</b> | <span style="white-space:nowrap">Metadata-Read</span>  | <span style="white-space:nowrap">Fast/Medium</span>     | <span style="white-space:nowrap">Number of Partitions</span> | No (most cases)             |
// MAGIC | <b>Tables</b>  | <span style="white-space:nowrap">n/a</span>            | <span style="white-space:nowrap">n/a</span>            | <span style="white-space:nowrap">Predefined</span> | n/a            |
// MAGIC | <b>JSON</b>    | <span style="white-space:nowrap">Full-Read-Data</span> | <span style="white-space:nowrap">Slow</span>            | <span style="white-space:nowrap">File size</span>  | Yes            |
// MAGIC | <b>Text</b>    | <span style="white-space:nowrap">Dictated</span>       | <span style="white-space:nowrap">Zero</span>            | <span style="white-space:nowrap">Only 1 Column</span>   | Never          |
// MAGIC | <b>JDBC</b>    | <span style="white-space:nowrap">DB-Read</span>        | <span style="white-space:nowrap">Fast</span>            | <span style="white-space:nowrap">DB Schema</span>  | No             |

// COMMAND ----------

// MAGIC %md
// MAGIC ##Reading CSV
// MAGIC - `spark.read.csv(..)`
// MAGIC - There are a large number of options when reading CSV files including headers, column separator, escaping, etc.
// MAGIC - We can allow Spark to infer the schema at the cost of first reading in the entire file.
// MAGIC - Large CSV files should always have a schema pre-defined.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading Parquet
// MAGIC - `spark.read.parquet(..)`
// MAGIC - Parquet files are the preferred file format for big-data.
// MAGIC - It is a columnar file format.
// MAGIC - It is a splittable file format.
// MAGIC - It offers a lot of performance benefits over other formats including predicate pushdown.
// MAGIC - Unlike CSV, the schema is read in, not inferred.
// MAGIC - Reading the schema from Parquet's metadata can be extremely efficient.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading Tables
// MAGIC - `spark.read.table(..)`
// MAGIC - The Databricks platform allows us to register a huge variety of data sources as tables via the Databricks UI.
// MAGIC - Any `DataFrame` (from CSV, Parquet, whatever) can be registered as a temporary view.
// MAGIC - Tables/Views can be loaded via the `DataFrameReader` to produce a `DataFrame`
// MAGIC - Tables/Views can be used directly in SQL statements.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading JSON
// MAGIC - `spark.read.json(..)`
// MAGIC - JSON represents complex data types unlike CSV's flat format.
// MAGIC - Has many of the same limitations as CSV (needing to read the entire file to infer the schema)
// MAGIC - Like CSV has a lot of options allowing control on date formats, escaping, single vs. multiline JSON, etc.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading Text
// MAGIC - `spark.read.text(..)`
// MAGIC - Reads one line of text as a single column named `value`.
// MAGIC - Is the basis for more complex file formats such as fixed-width text files.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading JDBC
// MAGIC - `spark.read.jdbc(..)`
// MAGIC - Requires one database connection per partition.
// MAGIC - Has the potential to overwhelm the database.
// MAGIC - Requires specification of a stride to properly balance partitions.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Reading Data Lab
// MAGIC It's time to put what we learned to practice.
// MAGIC 
// MAGIC Go ahead and open the notebook [Reading Data Lab]($./Reading Data 8 - Lab) and complete the exercises.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>