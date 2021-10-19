// Databricks notebook source
val newData = spark.sql("select * from amazon where time > ((select max(time) from amazon) - 86400)")

// COMMAND ----------

newData.coalesce(48).write.json("/mnt/jason/amazon/amazon-stream-input")

// COMMAND ----------

