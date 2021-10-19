# Databricks notebook source
# MAGIC %scala
# MAGIC import java.sql.DriverManager
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcPort = "1433"
# MAGIC val jdbcDatabase = "DEV-ASDW-Analytica-EDW"
# MAGIC val jdbcUsername = "sqldwadmin"
# MAGIC val jdbcPassword = "Symp01!*"
# MAGIC val jdbcHostname = "sqldw-ddev-01.database.windows.net"
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC //val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

# COMMAND ----------

# MAGIC %scala
# MAGIC val jdbcPort = "1433"
# MAGIC val jdbcDatabase = "DEV-ASDW-Analytica-EDW"
# MAGIC val jdbcUsername = "sqldwadmin"
# MAGIC val jdbcPassword = "Symp01!*"
# MAGIC val jdbcHostname = "sqldw-ddev-01.database.windows.net"
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC //val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

# COMMAND ----------

# MAGIC %scala
# MAGIC val conn = DriverManager.getConnection(jdbcUrl,jdbcUsername,jdbcPassword)
# MAGIC val proc_stmt = conn.prepareCall("""{call log.usp_ErrorLog('test18','test28','test38','test48','test58','test68','test78','TEST88','TEST98','TEST108','TEST118')}""")
# MAGIC proc_stmt.execute()
# MAGIC //proc_stmt.execute()