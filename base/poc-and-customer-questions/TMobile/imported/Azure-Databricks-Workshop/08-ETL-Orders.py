# Databricks notebook source
# MAGIC %md
# MAGIC ###Setup JDBC Connection

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %run ./Database-Settings

# COMMAND ----------

# MAGIC %run ./Includes/SQL-Database-Connector

# COMMAND ----------

# MAGIC %run ./Includes/SQL-Datawarehouse-Connector

# COMMAND ----------

# MAGIC %md
# MAGIC **O** will be all orders fabricated from the demo orders.

# COMMAND ----------

O = spark.read.parquet("/mnt/msft-training/initech/ordersTotal.parquet").withColumn("ModifiedDate", col("ModifiedDate").cast("timestamp"))

# COMMAND ----------

display(O)

# COMMAND ----------

O.createOrReplaceTempView("orders_total")

# COMMAND ----------

# MAGIC %md
# MAGIC **P** will be all OUR new products from the SalesLT.Product table.

# COMMAND ----------

P = spark.read.jdbc(jdbc_url_db, "SalesLT.Product").filter("ProductID > 7000")
display(P)

# COMMAND ----------

# MAGIC %md
# MAGIC **nOrdersDF** will be all new Orders w/ schema to match insert for SalesLT.SalesOrderDetail

# COMMAND ----------

nOrdersDF = (O.join(P, O.ProductID == P.ProductID).select(O.SalesOrderID, O.ProductID, O.rowguid, O.OrderQty, P.ListPrice.alias("UnitPrice"), O.UnitPriceDiscount, O.ModifiedDate)
      )

# COMMAND ----------

nOrdersDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Split Orders for Update to SQL-DB and for JSON

# COMMAND ----------

nSqlOrdersDF = nOrdersDF.sample(False, .6, 42)
nJsonOrdersDF = nOrdersDF.sample(False, .4, 42)

# COMMAND ----------

# MAGIC %md
# MAGIC Write New Orders To SQL DB
# MAGIC 
# MAGIC Drop rowguid to use auto-populate in DB

# COMMAND ----------

nSqlOrdersDF = nSqlOrdersDF.drop("rowguid")

# COMMAND ----------

display(nSqlOrdersDF)

# COMMAND ----------

ClassroomHelper.sql_update(jdbc_url_db, "DELETE FROM SalesLT.SalesOrderDetail WHERE ProductID >= 7724")
nSqlOrdersDF.write.jdbc(jdbc_url_db, "SalesLT.SalesOrderDetail", mode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC Write out the JSON split.

# COMMAND ----------

nJsonOrdersDF = (nJsonOrdersDF.drop("CustomerKey")
                .withColumn("ResellerKey", lit(701))
                .withColumn("EmployeeKey", lit(293))
                )

# COMMAND ----------

nJsonOrdersDF.write.mode("overwrite").json("/mnt/training-adl/PartnerOrder.json")

# COMMAND ----------

nJsonOrdersDF.printSchema()

# COMMAND ----------

# MAGIC %fs ls /mnt/training-msft/PartnerOrder.json