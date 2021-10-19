// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ### File Formats
// MAGIC 
// MAGIC In this notebook, we cover how different file formats impact your Spark Job performance.
// MAGIC 
// MAGIC Spark Summit 2016: [Why You Should Care about Data Layout in the Filesystem](https://databricks.com/session/why-you-should-care-about-data-layout-in-the-filesystem)

// COMMAND ----------

// MAGIC %run "./Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC Let's read in a colon delimited file.

// COMMAND ----------

// MAGIC %fs ls mnt/training/dataframes/people-with-header-10m.txt

// COMMAND ----------

// MAGIC %fs head --maxBytes=1000 mnt/training/dataframes/people-with-header-10m.txt

// COMMAND ----------

val csvDF = (spark
             .read
             .option("header", "true")
             .option("sep", ":")
             .csv("mnt/training/dataframes/people-with-header-10m.txt"))

// COMMAND ----------

// MAGIC %md
// MAGIC Are these data types correct? All of them are string types.
// MAGIC 
// MAGIC We need to tell Spark to infer the schema.

// COMMAND ----------

val csvDF = (spark
             .read
             .option("header", "true")
             .option("sep", ":")
             .option("inferSchema", "true")
             .csv("mnt/training/dataframes/people-with-header-10m.txt"))

// COMMAND ----------

// MAGIC %md
// MAGIC Wow, that took a long time just to figure out the schema for this file! 
// MAGIC 
// MAGIC Now let's try the same thing with compressed files (Gzip and Snappy formats).
// MAGIC 
// MAGIC Notice that the gzip file is the most compact - we will see if it is the fastest to operate on.

// COMMAND ----------

// MAGIC %fs ls mnt/training/dataframes/people-with-header-10m.txt

// COMMAND ----------

// MAGIC %fs ls mnt/training/dataframes/people-with-header-10m.txt.snappy

// COMMAND ----------

// MAGIC %fs ls mnt/training/dataframes/people-with-header-10m.txt.gz

// COMMAND ----------

// MAGIC %md
// MAGIC Read in the Snappy compression format file.

// COMMAND ----------

val csvDFsnappy = (spark
                 .read
                 .option("header", "true")
                 .option("sep", ":")
                 .option("inferSchema", "true")
                 .csv("mnt/training/dataframes/people-with-header-10m.txt.snappy"))

// COMMAND ----------

// MAGIC %md
// MAGIC Wait, I thought snappy format was supposed to be splittable - why was only one slot reading in the file?
// MAGIC 
// MAGIC Turns out Snappy format is only splittable for column based formats. If you want to work with non-column based formats, you should use `bzip2` (Snappy is great for Parquet, which we'll see later).

// COMMAND ----------

// MAGIC %fs ls /mnt/training/dataframes/tuning/people-with-header-10m.csv.bzip

// COMMAND ----------

// MAGIC %md
// MAGIC Wow! The bzip file actually takes up less space than the snappy or gzip file. Let's read it in.

// COMMAND ----------

val csvBzip = (spark
             .read
             .option("header", "true")
             .option("sep", ":")
             .option("inferSchema", "true")
             .csv("mnt/training/dataframes/tuning/people-with-header-10m.csv.bzip"))

// COMMAND ----------

// MAGIC %md
// MAGIC Look at how much faster that was! Note how many partitions it has now.
// MAGIC 
// MAGIC Now, let's read in the gzipped file.

// COMMAND ----------

val csvDFgz = (spark
             .read
             .option("header", "true")
             .option("sep", ":")
             .option("inferSchema", "true")
             .csv("mnt/training/dataframes/people-with-header-10m.txt.gz"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Although the uncompressed format took up more space than the Gzip format, it was significantly faster to operate on than the Gzip format. 
// MAGIC 
// MAGIC Let's dig into compression schemes and `inferSchema`...
// MAGIC 
// MAGIC How can we avoid this painful schema inference step?

// COMMAND ----------

csvDF.schema.json

// COMMAND ----------

import org.apache.spark.sql.types._
dbutils.fs.put(userhome + "/tmp/myschema.json", csvDF.schema.json, true)

val schema_json = dbutils.fs.head(userhome + "/tmp/myschema.json", Integer.MAX_VALUE)
val knownSchema = DataType.fromJson(schema_json).asInstanceOf[StructType]

// COMMAND ----------

val csvDFgz = (spark
             .read
             .option("header", "true")
             .option("sep", ":")
             .schema(knownSchema)
             .csv("mnt/training/dataframes/people-with-header-10m.txt.gz"))

// COMMAND ----------

// MAGIC %md
// MAGIC Much better, we loaded it in less than a second!
// MAGIC 
// MAGIC Now let's compare this CSV file to Parquet.

// COMMAND ----------

// MAGIC %fs ls mnt/training/dataframes/people-10m.parquet/

// COMMAND ----------

// MAGIC %python
// MAGIC size = [i.size for i in dbutils.fs.ls("mnt/training/dataframes/people-10m.parquet/") if i.name.endswith(".parquet")]
// MAGIC __builtin__.sum(size)

// COMMAND ----------

// MAGIC %md
// MAGIC In addition to the Parquet file taking up less than 1/2 of the space required to store the uncompressed text file, it also encodes the column names and their associated data types.
// MAGIC 
// MAGIC ***BONUS*** - Why did we go from 1 CSV file to 8 Parquet files??

// COMMAND ----------

val parquetDF = spark.read.parquet("mnt/training/dataframes/people-10m.parquet/")

// COMMAND ----------

// MAGIC %md
// MAGIC Lastly, it is much faster to operate on Parquet files than CSV files (especially when we are filtering or selecting a subset of columns). 
// MAGIC 
// MAGIC Look at the difference in times below! `%timeit` is a built-in Python function, so we are going to create temporary views to access the data in Python.

// COMMAND ----------

parquetDF.createOrReplaceTempView("parquetDF")
csvDF.createOrReplaceTempView("csvDF")
csvDFgz.createOrReplaceTempView("csvDFgz")

// COMMAND ----------

// MAGIC %python
// MAGIC %timeit -n1 -r1 spark.table("parquetDF").select("gender", "salary").where("salary > 10000").count()

// COMMAND ----------

// MAGIC %md
// MAGIC Due to DBIO Cache, if we re-run this query, look at how fast it is to operate on the Parquet file!

// COMMAND ----------

// MAGIC %python
// MAGIC %timeit -n1 -r1 spark.table("parquetDF").select("gender", "salary").where("salary > 10000").count()

// COMMAND ----------

// MAGIC %python
// MAGIC %timeit -n1 -r1 spark.table("csvDF").select("gender", "salary").where("salary > 10000").count()

// COMMAND ----------

// MAGIC %python
// MAGIC %timeit -n1 -r1 spark.table("csvDFgz").select("gender", "salary").where("salary > 10000").count()

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

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>