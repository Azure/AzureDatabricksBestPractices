# Databricks notebook source
# MAGIC %md
# MAGIC import SQL driver

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %md connect to Azure SQL Server

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcHostname = "xxxxxxxxxxxxxxx"
# MAGIC val jdbcPort = xxxx
# MAGIC val jdbcDatabase = "xxxx"
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

# MAGIC %scala
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %md Export data from Databricks to Azure SQL Server

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