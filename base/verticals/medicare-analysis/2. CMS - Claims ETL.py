# Databricks notebook source
# MAGIC %md
# MAGIC ### CMS Claims Dataset 
# MAGIC Source site: https://www.cms.gov/OpenPayments/index.html  
# MAGIC 
# MAGIC Link to datasets: https://www.cms.gov/OpenPayments/Explore-the-Data/Dataset-Downloads.html
# MAGIC 
# MAGIC We load 5.6GB CSV file into Spark to transform the dataset into a more efficient format. 

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lh /dbfs/mnt/mwc/cms/2015-payment/gen_records_aa

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 2 /dbfs/mnt/mwc/cms/2015-payment/gen_records_aa

# COMMAND ----------

df = spark.read.options(header='true').csv("/mnt/mwc/cms/2015-payment")
display(df)

# COMMAND ----------

df.coalesce(8).write.parquet("/mnt/mwc/cms_p/2015")

# COMMAND ----------

df = spark.read.parquet('/mnt/mwc/cms_p/2015')
df.count()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can start EDA and fraud detection methods in the following [notebook](#workspace/Users/mwc@databricks.com/demo/Medicare Analysis/3. Medicare Fraud Analysis)

# COMMAND ----------

