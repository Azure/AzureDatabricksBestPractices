# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Import SQL driver

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Set credentials for Azure SQL DB

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcUsername = "NDWPOCAdmin" //dbutils.secrets.get(scope = "jdbc", key = "username")
# MAGIC val jdbcPassword = "!" //dbutils.secrets.get(scope = "jdbc", key = "password")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Create connection properties 

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcHostname = "ndwpocsqldbsrv.database.windows.net"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "ndwpocsqldb"
# MAGIC 
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC 
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC connectionProperties.put("password", s"${jdbcPassword}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Set driver class

# COMMAND ----------

# MAGIC %scala
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLDW"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %md Export data from Databricks to Azure SQL Server

# COMMAND ----------

# MAGIC %md
# MAGIC Export Incremental

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC 
# MAGIC spark.table("v_dwh_association_cell_info_incremental")
# MAGIC      .write
# MAGIC      .mode(SaveMode.Append)
# MAGIC      .jdbc(jdbcUrl, "stg.table1", connectionProperties)
# MAGIC     .jdbc(jdbcUrl, "stg.table2", connectionProperties)
# MAGIC     .jdbc(jdbcUrl, "stg.table3", connectionProperties)

# COMMAND ----------

# MAGIC %md Export FactSiteDown

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC 
# MAGIC spark.table("fact_site_down")
# MAGIC      .write
# MAGIC      .mode(SaveMode.Append)
# MAGIC      .jdbc(jdbcUrl, "fact_site_down", connectionProperties)

# COMMAND ----------

# MAGIC %md Export ASSOCIATION_OSS_CELL_INFO data

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC 
# MAGIC spark.table("ASSOCIATION_OSS_CELL_INFO")
# MAGIC      .write
# MAGIC      .mode(SaveMode.Append)
# MAGIC      .jdbc(jdbcUrl, "ASSOCIATION_OSS_CELL_INFO", connectionProperties)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.sql.DriverManager
# MAGIC 
# MAGIC // Call sproc
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
# MAGIC val conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC val proc_stmt = conn.prepareCall("{ call dbo.insert_basic_test() }")
# MAGIC proc_stmt.execute()