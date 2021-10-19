// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Intro To DataFrames, Lab #3
// MAGIC ## De-Duping Data

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC In this exercise, we're doing ETL on a file we've received from some customer. That file contains data about people, including:
// MAGIC 
// MAGIC * first, middle and last names
// MAGIC * gender
// MAGIC * birth date
// MAGIC * Social Security number
// MAGIC * salary
// MAGIC 
// MAGIC But, as is unfortunately common in data we get from this customer, the file contains some duplicate records. Worse:
// MAGIC 
// MAGIC * In some of the records, the names are mixed case (e.g., "Carol"), while in others, they are uppercase (e.g., "CAROL"). 
// MAGIC * The Social Security numbers aren't consistent, either. Some of them are hyphenated (e.g., "992-83-4829"), while others are missing hyphens ("992834829").
// MAGIC 
// MAGIC The name fields are guaranteed to match, if you disregard character case, and the birth dates will also match. (The salaries will match, as well,
// MAGIC and the Social Security Numbers *would* match, if they were somehow put in the same format).
// MAGIC 
// MAGIC The file's path is: `dbfs:/mnt/training/dataframes/people-with-dups.txt`
// MAGIC 
// MAGIC Your job is to remove the duplicate records. The specific requirements of your job are:
// MAGIC 
// MAGIC * Remove duplicates. It doesn't matter which record you keep; it only matters that you keep one of them.
// MAGIC * Preserve the data format of the columns. For example, if you write the first name column in all lower-case, you haven't met this requirement.
// MAGIC * Write the result as a Parquet file, as designated by *destFile*.
// MAGIC * The final Parquet "file" must contain 8 part files (8 files ending in ".parquet").
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** The initial dataset contains 103,000 records.<br/>
// MAGIC The de-duplicated result haves 100,000 records.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %run "../Includes/Initialize-Labs"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Hints
// MAGIC 
// MAGIC * Use the <a href="http://spark.apache.org/docs/latest/api/scala/index.html" target="_blank">API docs</a>. Specifically, you might find 
// MAGIC   <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset" target="_blank">Dataset</a> and
// MAGIC   <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">functions</a> to be helpful.
// MAGIC * It's helpful to look at the file first, so you can check the format. `dbutils.fs.head()` (or just `%fs head`) is a big help here.

// COMMAND ----------

// ANSWER

val sourceFile = "dbfs:/mnt/training/dataframes/people-with-dups.txt"
val destFile = userhome + "/people.parquet"

// In case it already exists
dbutils.fs.rm(destFile, true)

// First, let's see what the file looks like.
println(dbutils.fs.head(sourceFile))

// COMMAND ----------

// ANSWER

// dropDuplicates() will likely introduce a shuffle, so it helps to reduce the number of post-shuffle partitions.
spark.conf.set("spark.sql.shuffle.partitions", 8)

// COMMAND ----------

// ANSWER

// Okay, now we can read this thing.

val df = spark
  .read
  .option("header", "true")
  .option("inferSchema", "true")
  .option("sep", ":")
  .csv(sourceFile)

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions._

val dedupedDF = df
  .select($"*",
      lower($"firstName").as("lcFirstName"),
      lower($"middleName").as("lcMiddleName"),
      lower($"lastName").as("lcLastName"),
      translate($"ssn", "-", "").as("ssnNums")
      // regexp_replace($"ssn", "-", "").as("ssnNums")
      // regexp_replace($"ssn", """^(\d{3})(\d{2})(\d{4})$""", "$1-$2-$3").alias("ssnNums")
   )
  .dropDuplicates("lcFirstName", "lcMiddleName", "lcLastName", "ssnNums", "gender", "birthDate", "salary")
  .drop("lcFirstName", "lcMiddleName", "lcLastName", "ssnNums")

// COMMAND ----------

// ANSWER

import org.apache.spark.sql.SaveMode

// Now we can save the results. We'll also re-read them and count them, just as a final check.
// Just for fun, we'll use the Snappy compression codec. It's not as compact as Gzip, but it's much faster.
dedupedDF.write
  .mode(SaveMode.Overwrite)
  .option("compression", "snappy")
  .parquet(destFile)

val parquetDF = spark.read.parquet(destFile)
printf("Total Records: %,d%n%n", parquetDF.count())

// COMMAND ----------

// ANSWER

display( dbutils.fs.ls(destFile) )

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer
// MAGIC 
// MAGIC At the bare minimum, we can verify that you wrote the parquet file out to **destFile** and that you have the right number of records.
// MAGIC 
// MAGIC Running the following cell to confirm your result:

// COMMAND ----------

val partFiles = dbutils.fs.ls(destFile).filter(_.path.endsWith(".parquet")).size

val finalDF = spark.read.parquet(destFile)
val finalCount = finalDF.count()

clearYourResults()
validateYourAnswer("01 Parquet File Exists", 1276280174, partFiles)
validateYourAnswer("02 Expected 100000 Records", 972882115, finalCount)
summarizeYourResults()


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>