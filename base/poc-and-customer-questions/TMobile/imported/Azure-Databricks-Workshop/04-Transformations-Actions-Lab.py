# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations and Actions Lab
# MAGIC * The goal of this lab is to put into practice some of what you have learned about reading data with Apache Spark.
# MAGIC * The instructions are provided below along with empty cells for you to do your work.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Instructions
# MAGIC 0. Start with the file **"dbfs:/mnt/training-msft/initech/Product.csv"**
# MAGIC 0. Apply the necessary transformations and actions to return a DataFrame which satisfies these requirements:
# MAGIC 0. Contains just the `product_id`, `category`, `brand`, `model` and `price` columns
# MAGIC 0. Rename `product_id` to `prodID`
# MAGIC 0. Sort by the `price` column in descending order
# MAGIC 0. Export as parquet format i.e. save to **"dbfs:/mnt/training-msft/initech/Products.parquet"**
# MAGIC 
# MAGIC What were the top 10 pages? (Hint: Use `limit`)

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>