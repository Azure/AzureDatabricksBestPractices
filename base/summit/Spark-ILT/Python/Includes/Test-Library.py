# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC def dbTest(id, expected, result):
# MAGIC   assert str(expected) == str(result), "{} does not equal expected {}".format(result, expected)

# COMMAND ----------

# MAGIC %scala
# MAGIC def dbTest[T](id: String, expected: T, result: => T, message: String = ""): Unit = {
# MAGIC   assert(result == expected, message)
# MAGIC }
# MAGIC displayHTML("Imported Test Library...") // suppress output