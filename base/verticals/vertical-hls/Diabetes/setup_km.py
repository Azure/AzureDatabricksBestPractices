# Databricks notebook source
# MAGIC %md
# MAGIC # Download Data set

# COMMAND ----------

# MAGIC %fs ls /mnt/wesley/dataset/medicare/diabetes/

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/wesley/dataset/medicare/diabetes",True)
dbutils.fs.mkdirs("dbfs:/mnt/wesley/dataset/medicare/diabetes")

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget "https://raw.githubusercontent.com/AvisekSukul/Regression_Diabetes/master/Custom%20Diabetes%20Dataset.csv"

# COMMAND ----------

# MAGIC %sh
# MAGIC mv "Custom Diabetes Dataset.csv" "custom_diabetes_dataset.csv"

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l

# COMMAND ----------

# MAGIC %sh
# MAGIC cp "custom_diabetes_dataset.csv" /dbfs/mnt/wesley/dataset/medicare/diabetes

# COMMAND ----------

df = spark.read.csv('/mnt/wesley/dataset/medicare/diabetes/custom_diabetes_dataset.csv', header=True, sep=',', inferSchema=True)

# COMMAND ----------

