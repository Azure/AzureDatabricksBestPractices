// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Introduction to DataFrames, Lab #1
// MAGIC ## Distinct Articles

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC In the cell provided below, write the code necessary to count the number of distinct articles in our data set.
// MAGIC 0. Copy and paste all you like from the previous notebook.
// MAGIC 0. Read in our parquet files.
// MAGIC 0. Apply the necessary transformations.
// MAGIC 0. Assign the count to the variable `totalArticles`
// MAGIC 0. Run the last cell to verify that the data was loaded correctly.
// MAGIC 
// MAGIC **Bonus**
// MAGIC 
// MAGIC If you recall from the beginning of the previous notebook, the act of reading in our parquet files will trigger a job.
// MAGIC 0. Define a schema that matches the data we are working with.
// MAGIC 0. Update the read operation to use the schema.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

// COMMAND ----------

// TODO
// Replace <<FILL_IN>> with your code.

val parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

val df = spark                     // Our SparkSession & Entry Point
  .read                            // Our DataFrameReader
  <<FILL_IN>>                      // Read in the parquet files
  <<FILL_IN>>                      // Reduce the columns to just the one
  <<FILL_IN>>                      // Produce a unique set of values

val totalArticles = df.<<FILL_IN>> // Identify the total number of records remaining.

printf("Distinct Articles: %,d%n%n", totalArticles)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Verify Your Work
// MAGIC Run the following cell to verify that your `DataFrame` was created properly.

// COMMAND ----------

val expected = 1783138
assert (totalArticles == expected, s"Expected the total to be ${expected} but found ${totalArticles}")


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>