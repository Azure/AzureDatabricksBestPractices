# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC course_name = "Databricks Delta"

# COMMAND ----------

# MAGIC %run "./Dataset-Mounts"

# COMMAND ----------

# MAGIC %run "./Test-Library"

# COMMAND ----------

# %run "./Table-Creator"
None

# COMMAND ----------

# %run "./Delta-Setup"
None

# COMMAND ----------

# MAGIC %run "./Create-User-DB"

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def deleteTables(database) :
# MAGIC   try:
# MAGIC     tables=spark.catalog.listTables(database)
# MAGIC   except:
# MAGIC     return
# MAGIC   
# MAGIC   for table in tables:
# MAGIC       spark.sql("DROP TABLE {}.{}".format(database, table.name))
# MAGIC 
# MAGIC def untilStreamIsReady(name):
# MAGIC   queries = list(filter(lambda query: query.name == name, spark.streams.active))
# MAGIC 
# MAGIC   if len(queries) == 0:
# MAGIC     print("The stream is not active.")
# MAGIC 
# MAGIC   else:
# MAGIC     while (queries[0].isActive and len(queries[0].recentProgress) == 0):
# MAGIC       pass # wait until there is any type of progress
# MAGIC 
# MAGIC     if queries[0].isActive:
# MAGIC       queries[0].awaitTermination(5)
# MAGIC       print("The stream is active and ready.")
# MAGIC     else:
# MAGIC       print("The stream is not active.")
# MAGIC 
# MAGIC None

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def deleteTables(database:String):Unit = {
# MAGIC   val tables = try { spark.catalog.listTables(database).collect.toSeq} 
# MAGIC   catch { case _: Throwable => Seq[org.apache.spark.sql.catalog.Table]() }
# MAGIC   
# MAGIC   for (table <- tables) {
# MAGIC       spark.sql("DROP TABLE %s.%s".format(database, table.name))
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def untilStreamIsReady(name:String):Unit = {
# MAGIC   val queries = spark.streams.active.filter(_.name == name)
# MAGIC 
# MAGIC   if (queries.length == 0) {
# MAGIC     println("The stream is not active.")
# MAGIC   } else {
# MAGIC     while (queries(0).isActive && queries(0).recentProgress.length == 0) {
# MAGIC       // wait until there is any type of progress
# MAGIC     }
# MAGIC 
# MAGIC     if (queries(0).isActive) {
# MAGIC       queries(0).awaitTermination(5*1000)
# MAGIC       println("The stream is active and ready.")
# MAGIC     } else {
# MAGIC       println("The stream is not active.")
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC displayHTML("""
# MAGIC <div>Declared various utility methods:</div>
# MAGIC <li>Declared <b style="color:green">deleteTables(<i>database:String</i>)</b> for database resets</li>
# MAGIC <li>Declared <b style="color:green">untilStreamIsReady(<i>name:String</i>)</b> to control workflow</li>
# MAGIC <br/>
# MAGIC <div>All done!</div>
# MAGIC """)

# COMMAND ----------

