-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Reading Data Lab
-- MAGIC * The goal of this lab is to put into practice some of what you have learned about reading data with Azure Databricks and Apache Spark.
-- MAGIC * The instructions are provided below along with empty cells for you to do your work.
-- MAGIC * At the bottom of this notebook are additional cells that will help verify that your work is accurate.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Challenges
-- MAGIC * Data is all over the place - reports, KPIs and DS is hard with bigger data from disparate sources on exiting tools
-- MAGIC 
-- MAGIC ### Why Initech Needs a Data Lake
-- MAGIC * Store both big and data in one location for all personas - Data Engineering, Data Science, Analysts 
-- MAGIC * They need to access this data in diffrent languages and tools - SQL, Python, Scala, Java, R with Notebooks, IDE, Power BI, Tableau, JDBC/ODBC
-- MAGIC 
-- MAGIC ### Azure Databricks Solutions
-- MAGIC * Azure Storage or Azure Data Lake - Is a place to store all data, big and small
-- MAGIC * Access both big (TB to PB) and small data easily with Databricks' scaleable clusters
-- MAGIC * Use Python, Scala, R, SQL, Java

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ####Azure Databricks for Batch ETL & Data Engineers 
-- MAGIC 
-- MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_de.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Instructions
-- MAGIC 0. Start with the file **dbfs:/mnt/training-sources/initech/productsCsv/product.csv**, a file containing product details.
-- MAGIC 0. Read in the data and assign it to a `DataFrame` named **productDF**.
-- MAGIC 0. Run the last cell to verify that the data was loaded correctly and to print its schema.
-- MAGIC 
-- MAGIC Bonus: Create the `DataFrame` and print its schema **without** executing a single job.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) The Data Source
-- MAGIC * For this exercise, we will be using a file called **products.csv**.
-- MAGIC * The data represents new products we are planning to add to our online store.
-- MAGIC * We can use **&percnt;head ...** to view the first few lines of the file.

-- COMMAND ----------

SET spark.sql.shuffle.partitions = 4

-- COMMAND ----------

-- MAGIC %fs head /mnt/training-sources/initech/productsCsv/product.csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step #1 - Create a SparkSQL table 
-- MAGIC * Change the name of the table to `your_name_products`

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW products
USING CSV
OPTIONS (path "/mnt/training-sources/initech/productsCsv/")

-- COMMAND ----------

-- DBTITLE 0,TO-DO
SELECT * FROM products

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step #2 - Create a SparkSQL table that infers the schema
-- MAGIC * Change the name of the table to your name
-- MAGIC * Add the extra `OPTIONS` - hint:
-- MAGIC   * `header`
-- MAGIC   * `inferSchema`

-- COMMAND ----------

-- DBTITLE 1,TO-DO
CREATE OR REPLACE TEMPORARY VIEW products
USING CSV
OPTIONS (path "/mnt/training-sources/initech/productsCsv/",
        --TO DO--
        )

-- COMMAND ----------

-- DBTITLE 1,TO-DO
SELECT * FROM products

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <h2 style="color:red">End of Lab 1 </h2>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Bonus - If You Finish Early
-- MAGIC * Write your own queries to analyze the product table (counts, group by, etc)
-- MAGIC * Create the `DataFrame` and print its schema **without** executing a single job. Hint: [Specify the schema manually](https://github.com/databricks/spark-csv#sql-api)
-- MAGIC * Write the data to DBFS in parquet (Note: Use the path `dbfs:/tmp/{YOUR-NAME}/output/`) - https://docs.azuredatabricks.net/spark/latest/spark-sql/language-manual/create-table.html

-- COMMAND ----------

-- DBTITLE 1,TO-DO
CREATE OR REPLACE TEMPORARY VIEW products (
  product_id int, 
   --TO-D0-- 
USING CSV
OPTIONS (path "/mnt/training-sources/initech/productsCsv/",
        --TO-D0-- 
        )