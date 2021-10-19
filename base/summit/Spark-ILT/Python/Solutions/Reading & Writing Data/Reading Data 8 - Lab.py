# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data Lab
# MAGIC * The goal of this lab is to put into practice some of what you have learned about reading data with Apache Spark.
# MAGIC * The instructions are provided below along with empty cells for you to do your work.
# MAGIC * At the bottom of this notebook are additional cells that will help verify that your work is accurate.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
# MAGIC 0. Start with the file **dbfs:/mnt/training/wikipedia/clickstream/2015_02_clickstream.tsv**, some random file you haven't seen yet.
# MAGIC 0. Read in the data and assign it to a `DataFrame` named **testDF**.
# MAGIC 0. Run the last cell to verify that the data was loaded correctly and to print its schema.
# MAGIC 0. The one untestable requirement is that you should be able to create the `DataFrame` and print its schema **without** executing a single job.
# MAGIC 
# MAGIC **Note:** For the test to pass, the following columns should have the specified data types:
# MAGIC  * **prev_id**: integer
# MAGIC  * **curr_id**: integer
# MAGIC  * **n**: integer
# MAGIC  * **prev_title**: string
# MAGIC  * **curr_title**: string
# MAGIC  * **type**: string

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Show Your Work

# COMMAND ----------

# ANSWER

# The students will actually need to do this in two steps.
fileName = "dbfs:/mnt/training/wikipedia/clickstream/2015_02_clickstream.tsv"

# The first step will be to use inferSchema = true 
# It's the only way to figure out what the column and data types are
(spark.read
  .option("sep", "\t")
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(fileName)
  .printSchema()
)

# COMMAND ----------

# ANSWER

from pyspark.sql.types import *

# The second step is to create the schema
schema = StructType([
    StructField("prev_id", IntegerType(), False),
    StructField("curr_id", IntegerType(), False),
    StructField("n", IntegerType(), False),
    StructField("prev_title", StringType(), False),
    StructField("curr_title", StringType(), False),
    StructField("type", StringType(), False)
])

fileName = "dbfs:/mnt/training/wikipedia/clickstream/2015_02_clickstream.tsv"

#The third step is to read the data in with the user-defined schema
testDF = (spark.read
  .option("sep", "\t")
  .option("header", "true")
  .schema(schema)
  .csv(fileName)
)

testDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Verify Your Work
# MAGIC Run the following cell to verify that your `DataFrame` was created properly.
# MAGIC 
# MAGIC **Remember:** This should execute without triggering a single job.

# COMMAND ----------

testDF.printSchema()

columns = testDF.dtypes
assert len(columns) == 6, "Expected 6 columns but found " + str(len(columns))

assert columns[0][0] == "prev_id",    "Expected column 0 to be \"prev_id\" but found \"" + columns[0][0] + "\"."
assert columns[0][1] == "int",        "Expected column 0 to be of type \"int\" but found \"" + columns[0][1] + "\"."

assert columns[1][0] == "curr_id",    "Expected column 1 to be \"curr_id\" but found \"" + columns[1][0] + "\"."
assert columns[1][1] == "int",        "Expected column 1 to be of type \"int\" but found \"" + columns[1][1] + "\"."

assert columns[2][0] == "n",          "Expected column 2 to be \"n\" but found \"" + columns[2][0] + "\"."
assert columns[2][1] == "int",        "Expected column 2 to be of type \"int\" but found \"" + columns[2][1] + "\"."

assert columns[3][0] == "prev_title", "Expected column 3 to be \"prev_title\" but found \"" + columns[3][0] + "\"."
assert columns[3][1] == "string",     "Expected column 3 to be of type \"string\" but found \"" + columns[3][1] + "\"."

assert columns[4][0] == "curr_title", "Expected column 4 to be \"curr_title\" but found \"" + columns[4][0] + "\"."
assert columns[4][1] == "string",     "Expected column 4 to be of type \"string\" but found \"" + columns[4][1] + "\"."

assert columns[5][0] == "type",       "Expected column 5 to be \"type\" but found \"" + columns[5][0] + "\"."
assert columns[5][1] == "string",     "Expected column 5 to be of type \"string\" but found \"" + columns[5][1] + "\"."

print("Congratulations, all tests passed... that is if no jobs were triggered :-)\n")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>