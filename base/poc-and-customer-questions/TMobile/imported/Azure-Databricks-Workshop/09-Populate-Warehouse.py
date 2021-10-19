# Databricks notebook source
# MAGIC %md
# MAGIC ###Setup JDBC Connection

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
# MAGIC ###Read Tables from Azure SQL Database that are related to orders.
# MAGIC   
# MAGIC   **`SalesLT.Product`** table contains details about the products we sell.

# COMMAND ----------

productDF = spark.read.jdbc(
  jdbc_url_db,                    # the JDBC URL
  table="saleslt.Product"          # the name of the table
)

productDF.printSchema()

# COMMAND ----------

display(productDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **`SalesLT.SalesOrderHeader`** table contains information about each individual order.

# COMMAND ----------

orderHeaderDF = spark.read.jdbc(
  url=jdbc_url_db,                    # the JDBC URL
  table="saleslt.salesorderheader"          # the name of the table
)

orderHeaderDF.printSchema()

# COMMAND ----------

display(orderHeaderDF)

# COMMAND ----------

# MAGIC %md
# MAGIC **`SalesLT.SalesOrderDetail`** table contains information about each item in an order.

# COMMAND ----------

orderDetailDF = spark.read.jdbc(
  url=jdbc_url_db,                    # the JDBC URL
  table="saleslt.salesorderdetail"          # the name of the table
)

orderDetailDF.printSchema()

# COMMAND ----------

display(orderDetailDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ####We need to transform the data in these tables to match the Azure Data Warehouse `FactInternetSales` Table Schema.
# MAGIC 
# MAGIC Let's start by connecting to the Azure Data Warehouse so we can look that the Schema for the **`FactInternetSales`** table.

# COMMAND ----------

salesDF = spark.read.jdbc(
  url=jdbc_url_dw,
  table="FactInternetSales"
)

sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's perform the joins and column name transformations necessary to produce a dataframe that will map to the schema of our **`FactInternetSales`** table in the Data Warehouse.

# COMMAND ----------

from pyspark.sql.functions import *
factInternetSalesJoinDF = (orderDetailDF.join(orderHeaderDF, orderHeaderDF.SalesOrderID == orderDetailDF.SalesOrderID).select(orderDetailDF.ProductID, orderDetailDF.OrderQty, orderDetailDF.UnitPrice, orderDetailDF.UnitPriceDiscount, orderHeaderDF.OrderDate.cast("bigint"), orderHeaderDF.DueDate.cast("bigint"), orderHeaderDF.ShipDate.cast("bigint"), orderHeaderDF.CustomerID, orderHeaderDF.SalesOrderNumber, orderHeaderDF.RevisionNumber, orderHeaderDF.SubTotal, orderHeaderDF.TaxAmt, orderHeaderDF.Freight, orderHeaderDF.PurchaseOrderNumber)
                      )

# COMMAND ----------

factInternetSalesDF = (factInternetSalesJoinDF
          .withColumn("OrderDateKey", from_unixtime(col("OrderDate"), 'yyyyMMdd').cast("integer") )
          .withColumn("DueDateKey", from_unixtime(col("DueDate"), 'yyyyMMdd').cast("integer") )
          .withColumn("ShipDateKey", from_unixtime(col("ShipDate"), 'yyyyMMdd').cast("integer") )
          .withColumn("PromotionKey", lit(1))
          .withColumn("CurrencyKey", lit(100))
          .withColumn("SalesTerritoryKey", lit(1))
          .withColumn("SalesOrderLineNumber", lit(1))
          .withColumn("DiscountAmount", lit(0))
          .withColumn("SalesAmount", lit(1))
          .withColumn("CarrierTrackingNumber", lit("NULL"))
          .drop("OrderDate")
          .drop("DueDate")
          .drop("ShipDate")
         )

# COMMAND ----------

display(factInternetSalesDF)

# COMMAND ----------

factInternetSalesFinalDF = (factInternetSalesDF.join(productDF, factInternetSalesDF.ProductID == productDF.ProductID)
            .select(factInternetSalesDF.ProductID.alias("ProductKey"), 
                    factInternetSalesDF.OrderDateKey, 
                    factInternetSalesDF.DueDateKey, 
                    factInternetSalesDF.ShipDateKey, 
                    factInternetSalesDF.CustomerID.alias("CustomerKey"),
                    factInternetSalesDF.PromotionKey,
                    factInternetSalesDF.CurrencyKey,
                    factInternetSalesDF.SalesTerritoryKey,
                    factInternetSalesDF.SalesOrderNumber,
                    factInternetSalesDF.SalesOrderLineNumber,
                    factInternetSalesDF.RevisionNumber, 
                    factInternetSalesDF.OrderQty.alias("OrderQuantity"), 
                    factInternetSalesDF.UnitPrice, 
                    factInternetSalesDF.UnitPrice.alias("ExtendedAmount"), 
                    factInternetSalesDF.UnitPriceDiscount.alias("UnitPriceDiscountPct"),
                    factInternetSalesDF.DiscountAmount,
                    productDF.StandardCost.alias("ProductStandardCost"), 
                    factInternetSalesDF.SubTotal.alias("TotalProductCost"),
                    factInternetSalesDF.SalesAmount,
                    factInternetSalesDF.TaxAmt, 
                    factInternetSalesDF.Freight,
                    factInternetSalesDF.CarrierTrackingNumber,
                    factInternetSalesDF.PurchaseOrderNumber.alias("CustomerPONumber"))
            .filter("ProductKey > 7000")
           )
factInternetSalesFinalDF.printSchema()

# COMMAND ----------

display(factInternetSalesFinalDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Now we are ready to update the **`FactInternetSales`** table in the Azure Data Warehouse

# COMMAND ----------

ClassroomHelper.sql_update(jdbc_url_dw, "DELETE FROM FactInternetSales WHERE ProductKey > 7000")
factInternetSalesFinalDF.write.jdbc(jdbc_url_dw, "FactInternetSales", mode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Next We Need To ETL The New Partner Sales.
# MAGIC   
# MAGIC   **`/mnt/training-msft/initech/PartnerOrder.json`** contains details about our partner sales.

# COMMAND ----------

# MAGIC %md
# MAGIC Our partner reseller writes json files to our Azure Blog Store to report sales.
# MAGIC 
# MAGIC We will start by reading in the latest sales using a known schema we can provide.

# COMMAND ----------

from pyspark.sql.types import *

jsonSchema = StructType([
  StructField("SalesOrderID", IntegerType(), True),
  StructField("ProductID", IntegerType(), True),
  StructField("rowguid", StringType(), True),
  StructField("OrderQty", IntegerType(), True),
  StructField("UnitPrice", FloatType(), True),
  StructField("UnitPriceDiscount", FloatType(), True),
  StructField("ModifiedDate", TimestampType(), True),
  StructField("ResellerKey", IntegerType(), True),
  StructField("EmployeeKey", IntegerType(), True)
 ])

# COMMAND ----------

partnerOrdersDF = (spark.read           
  .schema(jsonSchema) 
  .json("/mnt/training-msft/initech/PartnerOrder.json")
)

# COMMAND ----------

display(partnerOrdersDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can perfrom the same join.

# COMMAND ----------

FISPartner = (partnerOrdersDF.join(orderHeaderDF, orderHeaderDF.SalesOrderID == partnerOrdersDF.SalesOrderID).select(partnerOrdersDF.ProductID, partnerOrdersDF.OrderQty, partnerOrdersDF.UnitPrice, partnerOrdersDF.UnitPriceDiscount, orderHeaderDF.OrderDate.cast("bigint"), orderHeaderDF.DueDate.cast("bigint"), orderHeaderDF.ShipDate.cast("bigint"), orderHeaderDF.CustomerID, orderHeaderDF.SalesOrderNumber, orderHeaderDF.RevisionNumber, orderHeaderDF.SubTotal, orderHeaderDF.TaxAmt, orderHeaderDF.Freight, orderHeaderDF.PurchaseOrderNumber, partnerOrdersDF.ResellerKey, partnerOrdersDF.EmployeeKey)
                      )

# COMMAND ----------

FISPartnerExt = (FISPartner
          .withColumn("OrderDateKey", from_unixtime(col("OrderDate"), 'yyyyMMdd').cast("integer") )
          .withColumn("DueDateKey", from_unixtime(col("DueDate"), 'yyyyMMdd').cast("integer") )
          .withColumn("ShipDateKey", from_unixtime(col("ShipDate"), 'yyyyMMdd').cast("integer") )
          .withColumn("PromotionKey", lit(1))
          .withColumn("CurrencyKey", lit(100))
          .withColumn("SalesTerritoryKey", lit(1))
          .withColumn("SalesOrderLineNumber", lit(1))
          .withColumn("DiscountAmount", lit(0))
          .withColumn("SalesAmount", lit(1))
          .withColumn("CarrierTrackingNumber", lit("NULL"))
          .drop("OrderDate")
          .drop("DueDate")
          .drop("ShipDate")
         )

# COMMAND ----------

FISPartnerFinal = (FISPartnerExt.join(productDF, FISPartnerExt.ProductID == productDF.ProductID)
            .select(FISPartnerExt.ProductID.alias("ProductKey"), 
                    FISPartnerExt.OrderDateKey, 
                    FISPartnerExt.DueDateKey, 
                    FISPartnerExt.ShipDateKey,
                    FISPartnerExt.ResellerKey,
                    FISPartnerExt.EmployeeKey,
                    FISPartnerExt.PromotionKey,
                    FISPartnerExt.CurrencyKey,
                    FISPartnerExt.SalesTerritoryKey,
                    FISPartnerExt.SalesOrderNumber,
                    FISPartnerExt.SalesOrderLineNumber,
                    FISPartnerExt.RevisionNumber, 
                    FISPartnerExt.OrderQty.alias("OrderQuantity"), 
                    FISPartnerExt.UnitPrice, 
                    FISPartnerExt.UnitPrice.alias("ExtendedAmount"), 
                    FISPartnerExt.UnitPriceDiscount.alias("UnitPriceDiscountPct"),
                    FISPartnerExt.DiscountAmount,
                    productDF.StandardCost.alias("ProductStandardCost"), 
                    FISPartnerExt.SubTotal.alias("TotalProductCost"),
                    FISPartnerExt.SalesAmount,
                    FISPartnerExt.TaxAmt, 
                    FISPartnerExt.Freight,
                    FISPartnerExt.CarrierTrackingNumber,
                    FISPartnerExt.PurchaseOrderNumber.alias("CustomerPONumber"))
            .filter("ProductKey > 7000")
           )
FISPartnerFinal.printSchema()

# COMMAND ----------

display(FISPartnerFinal)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Now we are ready to update the **`FactResellerSales`** table in the Azure Data Warehouse

# COMMAND ----------

ClassroomHelper.sql_update(jdbc_url_dw, "DELETE FROM FactResellerSales WHERE ProductKey > 7000")
FISPartnerFinal.write.jdbc(jdbc_url_dw, "FactResellerSales", mode="append")