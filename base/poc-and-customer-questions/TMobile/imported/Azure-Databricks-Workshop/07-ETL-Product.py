# Databricks notebook source
# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %run ./Database-Settings

# COMMAND ----------

# MAGIC %run ./Includes/SQL-Database-Connector

# COMMAND ----------

# MAGIC %run ./Includes/SQL-Datawarehouse-Connector

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md # Populate the Azure SQL Database's ProductCategory table

# COMMAND ----------

existingProductCategoryDF = spark.read.jdbc(jdbc_url_db, "SalesLT.ProductCategory").filter(col("ProductCategoryId") <= 100).orderBy("ProductCategoryId")
display(existingProductCategoryDF)

# COMMAND ----------

from collections import namedtuple
from pyspark.sql.types import *

ProductCategory = namedtuple("ProductCategory", [
  "ProductCategoryID",
  "ParentProductCategoryID",
  "Name",
])

schema=StructType([
  StructField("ProductCategoryID", IntegerType()),
  StructField("ParentProductCategoryID", IntegerType()),
  StructField("Name", StringType()),
])
newProductCategoryDF = spark.createDataFrame([
    (101, None, "Computers"),
    (102, 101,  "Laptop"),
    (103, 101,  "Tablet"),
  ], schema)
display(newProductCategoryDF)

# COMMAND ----------

 # OLD VERSION: This version doesn't let you specify the ProductCategoryID due to IDENTITY_INSERT errors, you'd have to accept generated ones by not inserting a ProductCategoryID

# ClassroomHelper.sql_update(jdbc_url_db, "DELETE FROM SalesLT.ProductCategory WHERE ProductCategoryID > 100")
# newProductCategoryDF.write.jdbc(jdbc_url_db, "SalesLT.ProductCategory", mode='append')

# COMMAND ----------

ClassroomHelper.sql_update(jdbc_url_db, "DELETE FROM SalesLT.ProductCategory WHERE ProductCategoryID > 100")

(newProductCategoryDF.repartition(1).write
  .format("com.databricks.training.azure.sqlserver")
  .mode("append")
  .option("url", jdbc_url_db)
  .option("dbtable", "SalesLT.ProductCategory")
  .save())

# COMMAND ----------

productCategoryDF=spark.read.jdbc(jdbc_url_db, "SalesLT.ProductCategory").orderBy("ProductCategoryID")
display(productCategoryDF)

# COMMAND ----------

# MAGIC %md # Populate the Azure SQL Warehouse's Product table

# COMMAND ----------

existingProductDF = spark.read.jdbc(jdbc_url_db, "SalesLT.Product").filter(col("ProductID") < 7724).orderBy("ProductId")
display(existingProductDF)

# COMMAND ----------

display(spark.read.csv("/mnt/training-msft/initech/ProductFinal.csv", header=True, inferSchema=True))

# COMMAND ----------

# MAGIC %sql select * from initech.new_product where _c19 is not null

# COMMAND ----------

newProductDF.printSchema()

# COMMAND ----------

newProductDF = (
  spark.read.csv("/FileStore/tables/ProductFinal.csv", header=True, inferSchema=True)
  .withColumnRenamed("DiscountedDate", "DiscontinuedDate")
  .withColumn("SellStartDate", unix_timestamp(col("SellStartDate"), 'M/dd/yy H:mm').cast("timestamp") )
  .withColumn("SellEndDate", unix_timestamp(col("SellEndDate"), 'M/dd/yy H:mm').cast("timestamp") )
  .withColumn("DiscontinuedDate", unix_timestamp(col("DiscontinuedDate"), 'M/dd/yy H:mm').cast("timestamp") )
  .withColumn("ModifiedDate", unix_timestamp(col("ModifiedDate"), 'M/dd/yy H:mm').cast("timestamp") )
  .withColumn("ThumbNailPhoto", col("ThumbNailPhoto").cast("binary"))
  .withColumn("Name", substring(col("Name"),0,50))
  .withColumn("Size", regexp_replace(col("Size"), '".*', '"'))
  .withColumn("Color", substring(col("Color"),0,15))
  .drop("rowguid")
)
display(newProductDF)

# COMMAND ----------

# OLD VERSION: This version doesn't let you specify the ProductID due to IDENTITY_INSERT errors, you'd have to accept generated ones by not inserting a ProductID

# ClassroomHelper.sql_update(jdbc_url_db, "DELETE FROM SalesLT.ProductCategory WHERE ProductCategoryID > 100")
# newProductCategoryDF.write.jdbc(jdbc_url_db, "SalesLT.ProductCategory", mode='append')

# COMMAND ----------

# Remove past runs before loading
ClassroomHelper.sql_update(jdbc_url_db, "DELETE FROM SalesLT.Product WHERE ProductId >= 7724")

# NEW VERSION: This version uses a custom JDBC connector that issues "SET IDENTITY_INSERT <table> ON" before inserting data.
(newProductDF.write
  .format("com.databricks.training.azure.sqlserver")
  .mode("append")
  .option("url", jdbc_url_db)
  .option("dbtable", "SalesLT.Product")
  .save())

# COMMAND ----------

productDF=spark.read.jdbc(jdbc_url_db, "SalesLT.Product").orderBy("ProductID")
display(productDF)

# COMMAND ----------

# MAGIC %md # Populate the Azure SQL Warehouse's DimProductCategory

# COMMAND ----------

existingDimProductCategoryDF = spark.read.jdbc(jdbc_url_dw, "DimProductCategory").filter(col("ProductCategoryKey") <= 100).orderBy("ProductCategoryKey")
display(existingDimProductCategoryDF)

# COMMAND ----------

from collections import namedtuple
from pyspark.sql.types import *

schema=StructType([
  StructField("ProductCategoryKey",          IntegerType(), nullable=False),
  StructField("ProductCategoryAlternateKey", IntegerType(), nullable=False),
  StructField("EnglishProductCategoryName",  StringType(),  nullable=False),
  StructField("SpanishProductCategoryName",  StringType(),  nullable=True),
  StructField("FrenchProductCategoryName",   StringType(),  nullable=True),
])
newDimProductCategoryDF = spark.createDataFrame([
    (101, 101, "Computers", "Ordenadores", "Ordinateurs")
  ], schema)
display(newDimProductCategoryDF)

# COMMAND ----------

# Cleanup old runs before repopulating
ClassroomHelper.sql_update(jdbc_url_dw, "DELETE FROM DimProductCategory WHERE ProductCategoryKey > 100")

# NEW VERSION: This version uses an Azure PolyBase connector, it's faster for large numbers of writes, but slower if the # of writes is very small.
(newDimProductCategoryDF.write
  .format("com.databricks.training.azure.datawarehouse")
  .mode("append")
  .option("url", jdbc_url_dw)
  .option("blobStorageAccount", polybaseBlobStorageAccount)
  .option("blobName", polybaseBlobName)
  .option("accessKey", polybaseBlobAccessKey)
  .option("dbtable", "DimProductCategory")
  .save())

# COMMAND ----------

# Cleanup old runs before repopulating
ClassroomHelper.sql_update(jdbc_url_dw, "DELETE FROM DimProductCategory WHERE ProductCategoryKey > 100")

# OLD VERSION: This version is slower for large numbers of writes because it doesn't use the connector.
newDimProductCategoryDF.write.jdbc(jdbc_url_dw, "DimProductCategory", mode='append')

# COMMAND ----------

productCategoryDF=spark.read.jdbc(jdbc_url_dw, "DimProductCategory").orderBy("ProductCategoryKey")
display(productCategoryDF)

# COMMAND ----------

# MAGIC %md # Populate the Azure SQL DW's DimProductSubcategory table

# COMMAND ----------

existingProductSubcategoryDF = spark.read.jdbc(jdbc_url_dw, "DimProductSubcategory").filter(col("ProductCategoryKey") <= 100).orderBy("ProductSubcategoryKey")
display(existingProductSubcategoryDF)

# COMMAND ----------

from pyspark.sql.types import *

schema=StructType([
  StructField("ProductSubcategoryKey",          IntegerType(), nullable=False),
  StructField("ProductSubcategoryAlternateKey", IntegerType(), nullable=False),
  StructField("EnglishProductSubcategoryName",  StringType(),  nullable=False),
  StructField("SpanishProductSubcategoryName",  StringType(),  nullable=True),
  StructField("FrenchProductSubcategoryName",   StringType(),  nullable=True),
  StructField("ProductCategoryKey",             IntegerType(), nullable=False),
])

newProductCategoryDF = spark.createDataFrame([
  (102, 102, "Laptop", "Portátil", "Portable", 101),
  (103, 103, "Tablet", "Tableta", "Tablette", 101),
], schema)

display(newProductCategoryDF)

# COMMAND ----------

ClassroomHelper.sql_update(jdbc_url_dw, "DELETE FROM DimProductSubcategory WHERE ProductSubcategoryKey > ?", 100)
newProductCategoryDF.write.jdbc(jdbc_url_dw, "DimProductSubcategory", mode='append')

# COMMAND ----------

productCategoryDF = spark.read.jdbc(jdbc_url_dw, "DimProductSubcategory").orderBy("ProductSubcategoryKey")
display(productCategoryDF)

# COMMAND ----------

# MAGIC %md # Populate the Azure SQL DW's DimProduct table

# COMMAND ----------

existingDimProductDF = spark.read.jdbc(jdbc_url_dw, "DimProduct").filter(col("ProductSubcategoryKey")<=100).orderBy(col("ProductKey"))
existingDimProductDF.printSchema()

# COMMAND ----------

display(existingProductDF)

# COMMAND ----------

newProductDF=(
  spark.table("initech.new_product").select(
    (col("ProductID")+700).alias("ProductKey"),
    col("ProductNumber").alias("ProductAlternateKey"),
    lit(102).alias("ProductSubcategoryKey"),
    lit(None).cast(StringType()).alias("WeightUnitMeasureCode"),
    lit("IN").alias("SizeUnitMeasureCode"),
    substring(col("Name"), 0, 50).alias("EnglishProductName"),
    substring(col("Name"), 0, 50).alias("SpanishProductName"),
    substring(col("Name"), 0, 50).alias("FrenchProductName"),
    col("StandardCost"),
    lit(True).alias("FinishedGoodsFlag"),
    col("Color"),
    lit(5).alias("SafetyStockLevel"),
    lit(3).alias("ReorderPoint"),
    col("ListPrice"),
    col("Size"),
    col("Size").alias("SizeRange"),
    col("Weight"),
    lit(30).alias("DaysToManufacture"),
    lit("C").alias("ProductLine"),
    (col("ListPrice") * 0.80).alias("DealerPrice"),
    lit("X").alias("Class"),
    lit("S").alias("Style"),
    lit("Model Name").alias("ModelName"),
    col("Name").alias("EnglishDescription"),
    col("Name").alias("FrenchDescription"),
    col("Name").alias("ChineseDescription"),
    col("Name").alias("ArabicDescription"),
    col("Name").alias("HebrewDescription"),
    col("Name").alias("ThaiDescription"),
    col("Name").alias("GermanDescription"),
    col("Name").alias("JapaneseDescription"),
    col("Name").alias("TurkishDescription"),
    col("SellStartDate").alias("StartDate"),
    col("SellEndDate").alias("EndDate"),
    when(col("SellEndDate") == None, "Current").otherwise(None).alias("Status")
  )
)
display(newProductDF)

# COMMAND ----------

ClassroomHelper.sql_update(jdbc_url_dw, "DELETE FROM DimProduct WHERE ProductKey > 700")
newProductDF.write.jdbc(jdbc_url_dw, "DimProduct", mode='append')

# COMMAND ----------

productDF = spark.read.jdbc(jdbc_url_dw, "DimProduct").orderBy("ProductKey")
display(productDF.filter("ProductKey > 700"))