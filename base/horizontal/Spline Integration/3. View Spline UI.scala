// Databricks notebook source
// MAGIC %sh
// MAGIC #kill any running version
// MAGIC kill $(jps | grep spline | awk '{print $1}')

// COMMAND ----------

// MAGIC %sh
// MAGIC unset JAVA_OPTS
// MAGIC java -jar /dbfs/FileStore/tables/spline_ui_0_2_2_exec-722ee.jar -Dspline.mongodb.url=mongodb://localhost -Dspline.mongodb.name=my_lineage_database_name -httpPort=8080

// COMMAND ----------

// MAGIC %md Go to [/driver-proxy/o/0/spline/8084/dashboard](/driver-proxy/o/0/spline/8084/dashboard) after running previous cell

// COMMAND ----------

// MAGIC %sh mongo --eval 'db=db.getSiblingDB("my_lineage_database_name");db.lineages.find();'

// COMMAND ----------

