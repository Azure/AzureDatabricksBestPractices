# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
# MAGIC 
# MAGIC In this exercise, we're doing ETL on a file we've received from some customer. That file contains data about people, including:
# MAGIC 
# MAGIC * first, middle and last names
# MAGIC * gender
# MAGIC * birth date
# MAGIC * Social Security number
# MAGIC * salary
# MAGIC 
# MAGIC But, as is unfortunately common in data we get from this customer, the file contains some duplicate records. Worse:
# MAGIC 
# MAGIC * In some of the records, the names are mixed case (e.g., "Carol"), while in others, they are uppercase (e.g., "CAROL"). 
# MAGIC * The Social Security numbers aren't consistent, either. Some of them are hyphenated (e.g., "992-83-4829"), while others are missing hyphens ("992834829").
# MAGIC 
# MAGIC The name fields are guaranteed to match, if you disregard character case, and the birth dates will also match. (The salaries will match, as well,
# MAGIC and the Social Security Numbers *would* match, if they were somehow put in the same format).
# MAGIC 
# MAGIC The file's path is: `/mnt/training/dataframes/tuning/people.csv`
# MAGIC 
# MAGIC Your job is to remove the duplicate records. The specific requirements of your job are:
# MAGIC 
# MAGIC * Remove duplicates. It doesn't matter which record you keep; it only matters that you keep one of them.
# MAGIC * Preserve the data format of the columns. That is, if you write the first name column in all lower-case, you haven't met this requirement.
# MAGIC * Write the result as a Parquet file, to `dbfs:/{userhome}/tmp/people.parquet`
# MAGIC 
# MAGIC In Part II, we will handle the skew in this dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom_Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Hints
# MAGIC 
# MAGIC * Use the <a href="http://spark.apache.org/docs/latest/api/python/index.html" target="_blank">API docs</a>. Specifically, you might find 
# MAGIC   <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame" target="_blank">DataFrame</a> and
# MAGIC   <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">functions</a> to be helpful.
# MAGIC * It's helpful to look at the file first, so you can check the format. `dbutils.fs.head()` (or just `%fs head`) is a big help here.

# COMMAND ----------

sourceFile = "/mnt/training/dataframes/tuning/people.csv"
destFile = userhome + "/tmp/people.parquet"

dbutils.fs.rm(destFile, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Step 2: Joins
# MAGIC 
# MAGIC Your job is to write a joined DF (self join on `ssn` column) to storage.
# MAGIC You will need to optimize this self join to reduce the total time to do this join and write operation to storage from ~2min to under 30seconds
# MAGIC 
# MAGIC #### HINTS: 
# MAGIC * Look at the distribution of `ssn` accross the dataset
# MAGIC * Look at the task time for the bottle neck 

# COMMAND ----------

df1 = dedupedDF.select("firstName" , "ssn")
df2 = dedupedDF.select("birthDate" , "ssn", "salary")

# COMMAND ----------

# TODO: join on snn
df_joined = df1.join(df2, ...

# COMMAND ----------

# TODO
# Write df_joined to storage
dfJoined.write...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise: 
# MAGIC Optimize this join's bottlenecks... reduce time from ~3min to ~50seconds

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>