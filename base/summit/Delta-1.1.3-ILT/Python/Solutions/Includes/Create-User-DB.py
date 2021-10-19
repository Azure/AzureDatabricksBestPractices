# Databricks notebook source
# MAGIC 
# MAGIC %scala
# MAGIC val databaseName = {
# MAGIC   val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC   val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC   val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
# MAGIC   val databaseName   = username.replaceAll("[^a-zA-Z0-9]", "_") + "_dbs"
# MAGIC   spark.conf.set("com.databricks.training.spark.databaseName", databaseName)
# MAGIC   databaseName
# MAGIC }
# MAGIC 
# MAGIC displayHTML(s"Created user-specific database")

# COMMAND ----------

# MAGIC %python
# MAGIC databaseName = spark.conf.get("com.databricks.training.spark.databaseName")
# MAGIC databaseName = databaseName[:len(databaseName)-1]+"p"
# MAGIC 
# MAGIC None #Suppress output

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS `{}`".format(databaseName))
spark.sql("USE `{}`".format(databaseName))

displayHTML("""Using the database <b style="color:green">{}</b>.""".format(databaseName))
