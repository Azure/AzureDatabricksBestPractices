# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Delta Batch Operations - Upsert
# MAGIC 
# MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Use Databricks Delta to UPSERT data into existing Databricks Delta tables
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers 
# MAGIC * Secondary Audience: Data Analysts and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC We will use online retail datasets from
# MAGIC * `/mnt/training/online_retail` in the demo part and
# MAGIC * `/mnt/training/structured-streaming/events/` in the exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

basePath            = userhome + "/delta/python"
deltaMiniDataPath   = basePath + "/customer-data-mini/"

# Configure our shuffle partitions for these exercises
spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## UPSERT 
# MAGIC 
# MAGIC Literally means "UPdate" and "inSERT". It means to atomically either insert a row, or, if the row already exists, UPDATE the row.
# MAGIC 
# MAGIC Alter data by changing the values in one of the columns for a specific `CustomerID`.
# MAGIC 
# MAGIC Let's load the CSV file `../outdoor-products-mini.csv`.

# COMMAND ----------

inputSchema = "InvoiceNo INT, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING"

miniDataDF = (spark.read          
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)                            
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT Using Non-Databricks Delta Pipeline
# MAGIC 
# MAGIC This feature is not supported in non-Delta pipelines.
# MAGIC 
# MAGIC To UPSERT means to "UPdate" and "inSERT". In other words, UPSERT is not an atomic operation. It is literally TWO operations. 
# MAGIC 
# MAGIC Running an UPDATE could invalidate data that is accessed by the subsequent INSERT operation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPSERT Using Databricks Delta Pipeline
# MAGIC 
# MAGIC Using Databricks Delta, however, we can do UPSERTS.

# COMMAND ----------

(miniDataDF
  .write
  .mode("overwrite")
  .format("delta")
  .save(deltaMiniDataPath) 
)

spark.sql("""
    DROP TABLE IF EXISTS customer_data_delta_mini
  """)

spark.sql("""
    CREATE TABLE customer_data_delta_mini
    USING DELTA 
    LOCATION '{}' 
  """.format(deltaMiniDataPath))

# COMMAND ----------

# MAGIC %md
# MAGIC List all rows with `CustomerID=20993`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Form a new DataFrame where `StockCode` is `99999` for `CustomerID=20993`.
# MAGIC 
# MAGIC Create a table `customer_data_delta_to_upsert` that contains this data.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You need to convert `InvoiceNo` to a `String` because Delta infers types and `InvoiceNo` looks like it should be an integer.

# COMMAND ----------

from pyspark.sql.functions import lit, col
customerSpecificDF = (miniDataDF
  .filter("CustomerID=20993")
  .withColumn("StockCode", lit(99999))
  .withColumn("InvoiceNo", col("InvoiceNo").cast("String")) 
 )

spark.sql("DROP TABLE IF EXISTS customer_data_delta_to_upsert")
customerSpecificDF.write.saveAsTable("customer_data_delta_to_upsert")

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert the new data into `customer_data_delta_mini`.
# MAGIC 
# MAGIC Upsert is done using the `MERGE INTO` syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customer_data_delta_mini
# MAGIC USING customer_data_delta_to_upsert
# MAGIC ON customer_data_delta_mini.CustomerID = customer_data_delta_to_upsert.CustomerID
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     customer_data_delta_mini.StockCode = customer_data_delta_to_upsert.StockCode
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
# MAGIC   VALUES (
# MAGIC     customer_data_delta_to_upsert.InvoiceNo,
# MAGIC     customer_data_delta_to_upsert.StockCode, 
# MAGIC     customer_data_delta_to_upsert.Description, 
# MAGIC     customer_data_delta_to_upsert.Quantity, 
# MAGIC     customer_data_delta_to_upsert.InvoiceDate, 
# MAGIC     customer_data_delta_to_upsert.UnitPrice, 
# MAGIC     customer_data_delta_to_upsert.CustomerID, 
# MAGIC     customer_data_delta_to_upsert.Country)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how this data is seamlessly incorporated into `customer_data_delta_mini`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_data_delta_mini WHERE CustomerID=20993

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Create a DataFrame out of the table `demo_iot_data_delta`.

# COMMAND ----------

# ANSWER
newDataDF = spark.sql("SELECT * FROM demo_iot_data_delta")

# COMMAND ----------

# TEST  - Run this cell to test your solution.
schema = str(newDataDF.schema)

dbTest("assert-1", True, "action,StringType" in schema)
dbTest("assert-2", True, "time,LongType" in schema)
dbTest("assert-3", True, "date,DateType" in schema)
dbTest("assert-4", True, "deviceId,IntegerType" in schema)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC Create another dataframe where you change`action` to `Close` for `date = '2018-06-01' ` and `deviceId = 485`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use `distinct`.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Consider using `selectExpr()`, as we did in [Lesson 3]($./03-Append).

# COMMAND ----------

# ANSWER
newDeviceId485DF = (newDataDF
  .selectExpr(" 'Close' as Action", "time", "date", "deviceId")
  .distinct()
  .filter("date = '2018-06-01' ")
  .filter("deviceId = 485")
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
actionCount = newDeviceId485DF.select("Action").count()

dbTest("Delta-L4-actionCount", 1, actionCount)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Exercise 3
# MAGIC 
# MAGIC Write to a new Databricks Delta table that contains just our data to be upserted.
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You can adapt the SQL syntax for the upsert from our demo example, above.

# COMMAND ----------

# ANSWER
spark.sql("DROP TABLE IF EXISTS iot_data_delta_to_upsert")
newDeviceId485DF.write.saveAsTable("iot_data_delta_to_upsert")

# COMMAND ----------

# TEST - Run this cell to test your solution.
try:
  tableExists = (spark.table("iot_data_delta_to_upsert") is not None)
  count = spark.table("iot_data_delta_to_upsert").count()
except:
  tableExists = False
  
dbTest("Delta-04-demoIotTableExists", True, tableExists)  
dbTest("Delta-04-demoIotTableHasRow", 1, count)  


print("Tests passed!")

# COMMAND ----------

# MAGIC %sql
# MAGIC --ANSWER
# MAGIC MERGE INTO demo_iot_data_delta
# MAGIC USING iot_data_delta_to_upsert
# MAGIC ON demo_iot_data_delta.deviceId = iot_data_delta_to_upsert.deviceId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     demo_iot_data_delta.action = iot_data_delta_to_upsert.action
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (action, time, date, deviceId)
# MAGIC   VALUES (
# MAGIC     iot_data_delta_to_upsert.action, 
# MAGIC     iot_data_delta_to_upsert.time, 
# MAGIC     iot_data_delta_to_upsert.date, 
# MAGIC     iot_data_delta_to_upsert.deviceId 
# MAGIC )

# COMMAND ----------

spark.sql("SELECT * FROM demo_iot_data_delta").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4
# MAGIC 
# MAGIC Count the number of items in `demo_iot_data_delta` where the `deviceId` is `485` and `action` is `Close`.

# COMMAND ----------

# ANSWER
count = spark.sql("SELECT count(*) as total FROM demo_iot_data_delta WHERE deviceId = 485 AND action = 'Close' ").collect()[0][0]

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("Delta-L4-demoiot-count", 17, count)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC In this Lesson, we used Databricks Delta to UPSERT data into existing Databricks Delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Table Batch Read and Writes</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>