# Databricks notebook source
# MAGIC %scala
# MAGIC package com.databricks.training
# MAGIC object ClassroomHelper {
# MAGIC   def sql_update(url : String, sql : String, args : Any*) : Unit = {
# MAGIC     val conn = java.sql.DriverManager.getConnection(url)
# MAGIC     try {
# MAGIC       val ps = conn.prepareStatement(sql)
# MAGIC       args.zipWithIndex.foreach {case (arg, i) => {
# MAGIC         ps.setObject(i+1, arg)
# MAGIC       }}
# MAGIC       ps.executeUpdate()
# MAGIC     } finally {
# MAGIC       conn.close()
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   def sql_update(url : String, sql : String, args : java.util.ArrayList[Object]) : Unit = {
# MAGIC     import scala.collection.JavaConverters._
# MAGIC     return sql_update(url, sql, args.asScala:_*)
# MAGIC   }
# MAGIC   
# MAGIC   def sql(url : String, sql : String) : Unit = {
# MAGIC     val conn = java.sql.DriverManager.getConnection(url)
# MAGIC     val stmt = conn.createStatement()
# MAGIC     val cmds = sql.split(";")
# MAGIC     var count = 0;
# MAGIC     try {
# MAGIC       for (cmd <- cmds) {
# MAGIC         if (!cmd.trim().isEmpty()) {
# MAGIC           stmt.addBatch(cmd)
# MAGIC           count += 1
# MAGIC         }
# MAGIC       }
# MAGIC       stmt.executeBatch()
# MAGIC     } finally {
# MAGIC       conn.close()
# MAGIC     }
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.training.ClassroomHelper
# MAGIC displayHTML("") //Supress Output

# COMMAND ----------

# MAGIC %python
# MAGIC # ALL_NOTEBOOKS
# MAGIC class ClassroomHelper(object):
# MAGIC   @staticmethod
# MAGIC   def sql_update(url, sql, *args):
# MAGIC     spark._jvm.__getattr__("com.databricks.training.ClassroomHelper$").__getattr__("MODULE$").sql_update(url, sql, args)
# MAGIC   @staticmethod
# MAGIC   def sql(url, sql):
# MAGIC     spark._jvm.__getattr__("com.databricks.training.ClassroomHelper$").__getattr__("MODULE$").sql(url, sql)
# MAGIC 
# MAGIC None #Suppress output

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.spearfishtrainingstorage.blob.core.windows.net",
  "cJoNObBZt8C8xz2GwQmaHx25DmyRXAyd8TEp7/HDlT6jgt4+LeOjwYEhQ5SsCCrO0HRy6xlL8WZEM6xEwE9+9Q==")

blobStoreBaseURL = "wasbs://training-container-clean@spearfishtrainingstorage.blob.core.windows.net/"

spark.conf.set("spark.databricks.delta.preview.enabled", True)

None

# COMMAND ----------

print ""
print "IMPORTANT: You can safely ignore any Warning messages above."
print "Setup Complete."