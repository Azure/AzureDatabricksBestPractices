# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC course_name = "Spark ILT"

# COMMAND ----------

# MAGIC %run "./Dataset-Mounts"

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC //*******************************************
# MAGIC // CHECK FOR REQUIRED VERIONS OF SPARK & DBR
# MAGIC //*******************************************
# MAGIC 
# MAGIC val dbrVersion = assertDbrVersion(4, 0)
# MAGIC val sparkVersion = assertSparkVersion(2, 3)
# MAGIC 
# MAGIC displayHTML(s"""
# MAGIC Checking versions...
# MAGIC   <li>Spark: $sparkVersion</li>
# MAGIC   <li>DBR: $dbrVersion</li>
# MAGIC   <li>Scala: $scalaVersion</li>
# MAGIC   <li>Python: ${spark.conf.get("com.databricks.training.python-version")}</li>
# MAGIC """)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbrVersion = assertDbrVersion(4, 0)
# MAGIC sparkVersion = assertSparkVersion(2, 3)
# MAGIC 
# MAGIC None # suppress output

# COMMAND ----------

# MAGIC %scala
# MAGIC displayHTML("All done!")