# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM aweaver.turbine_gold VERSION AS OF 3
# MAGIC EXCEPT ALL
# MAGIC SELECT * FROM aweaver.turbine_gold@v0

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY aweaver.turbine_gold