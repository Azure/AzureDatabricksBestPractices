# Databricks notebook source
# MAGIC %md
# MAGIC ###Explore Single Family Mortgages from the Census Tract Dataset 2010-2015
# MAGIC 
# MAGIC **Business case:**  
# MAGIC _TEXT_
# MAGIC   
# MAGIC **Problem Statement:**  
# MAGIC _TEXT_
# MAGIC   
# MAGIC **Business solution:**  
# MAGIC _TEXT_
# MAGIC   
# MAGIC **Technical Solution:**  
# MAGIC _TEXT_
# MAGIC   
# MAGIC   
# MAGIC Owner: Silvio  
# MAGIC Runnable: True   
# MAGIC Last Spark Version: 2.1  

# COMMAND ----------

# MAGIC %md 
# MAGIC [Dataset Reference - https://www.fhfa.gov/DataTools/Downloads/Pages/Public-Use-Databases.aspx](https://www.fhfa.gov/DataTools/Downloads/Pages/Public-Use-Databases.aspx)
# MAGIC 
# MAGIC [Schema - https://www.fhfa.gov/DataTools/Downloads/Documents/Enterprise-PUDB/Single-Family_Census_Tract_File_/2015_Single_Family_Census_Tract_File.pdf](https://www.fhfa.gov/DataTools/Downloads/Documents/Enterprise-PUDB/Single-Family_Census_Tract_File_/2015_Single_Family_Census_Tract_File.pdf)
# MAGIC 
# MAGIC 
# MAGIC 1. Ingest the Dataset
# MAGIC 2. Explore the Dataset
# MAGIC 3. Enrich the Dataset
# MAGIC 4. Apply ML to the Dataset
# MAGIC 
# MAGIC ###1. Ingest the Dataset

# COMMAND ----------

# MAGIC %fs ls /home/silvio/mortgage_data

# COMMAND ----------

# DBTITLE 1,About ~2.4GB of CSV
sum(map(lambda x: x.size, dbutils.fs.ls("/home/silvio/mortgage_data")))

# COMMAND ----------

# DBTITLE 1,What Does The Data Look Like?
# MAGIC %fs head /home/silvio/mortgage_data/sfh2.csv

# COMMAND ----------

# DBTITLE 1,Load The CSV Files From S3
sfhDf = sqlContext.read.format('csv').option("header", "true").option("delimiter", "\t").option("inferSchema", "true").load("/home/silvio/mortgage_data")

# COMMAND ----------

sfhDf.printSchema()

# COMMAND ----------

# DBTITLE 1,How Many Records
sfhDf.count()

# COMMAND ----------

# MAGIC %md ###2. Explore the Dataset Using PySpark

# COMMAND ----------

display(sfhDf)

# COMMAND ----------

display(sfhDf.select("unpaid_balance"))

# COMMAND ----------

display(sfhDf.select("unpaid_balance").describe())

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as f
display(sfhDf.groupBy("year").agg(f.avg("unpaid_balance")).orderBy("year"))

# COMMAND ----------

# DBTITLE 1,Create a SQL View over the DataFrame
sfhDf.createOrReplaceTempView("mortgages")

# COMMAND ----------

# DBTITLE 1,Run The Same Query Using SQL
# MAGIC %sql select year, avg(unpaid_balance) from mortgages group by year order by year asc

# COMMAND ----------

# MAGIC %md ###3. Enrich the Dataset

# COMMAND ----------

states = sqlContext.read.jdbc("jdbc:mysql://databricks-customer-example.chlee7xx28jo.us-west-2.rds.amazonaws.com:3306?user=root&password=tMFqirgjz1lc", "databricks.fmstates")

# COMMAND ----------

display(states)

# COMMAND ----------

display(states.groupBy("usps_code", "county_code").count())

# COMMAND ----------

spark.catalog.dropGlobalTempView("mortgages_with_geolocation")
spark.conf.set("spark.sql.shuffle.partitions", 16)
withGeo = sfhDf.join(states, ["usps_code", "county_code"])
withGeo.createGlobalTempView("mortgages_with_geolocation")

# COMMAND ----------

withGeo.printSchema()

# COMMAND ----------

# MAGIC %sql cache table global_temp.mortgages_with_geolocation

# COMMAND ----------

# MAGIC %sql select state_abbr, avg(unpaid_balance) as unpaid 
# MAGIC from global_temp.mortgages_with_geolocation group by state_abbr

# COMMAND ----------

# MAGIC %sql select state_abbr, avg(unpaid_balance) as unpaid 
# MAGIC from global_temp.mortgages_with_geolocation  
# MAGIC where year ="$year" group by state_abbr

# COMMAND ----------

# MAGIC %md ### 4. Query with Tableau

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC displayHTML("""
# MAGIC Connect Tableau to this cluster by using:<p><p/>
# MAGIC <b>Server</b>: demo.cloud.databricks.com<br/>
# MAGIC <b>Port</b>: 443<br/>
# MAGIC <b>HTTP Path</b>: sql/protocolv1/o/0/${dbutils.notebook.getContext.clusterId.get}<br/>
# MAGIC <b>Database</b>: default<br/>
# MAGIC <b>Custom SQL Query</b>: <code>select * from global_temp.mortgages_with_geolocation</code>
# MAGIC """)

# COMMAND ----------

# MAGIC %md ###5. Apply ML to the Dataset

# COMMAND ----------

from pyspark.ml.regression import *
from pyspark.ml.feature import *
from pyspark.ml import *
from pyspark.ml.tuning import *
from pyspark.ml.evaluation import *

cols = sfhDf.columns
cols.remove('unpaid_balance')

va = VectorAssembler(inputCols= cols, outputCol = 'features')
dt = DecisionTreeRegressor(maxDepth=10, varianceCol="variance", labelCol='unpaid_balance')
re = RegressionEvaluator(predictionCol="prediction", labelCol="unpaid_balance", metricName="mae")
tvs = TrainValidationSplit(estimator=dt, evaluator=re, seed=42, estimatorParamMaps=ParamGridBuilder().addGrid(dt.maxDepth, [10]).build())
pipeline = Pipeline(stages = [va, tvs])
model = pipeline.fit(withGeo)

# COMMAND ----------

zip(range(0, len(cols)), cols)

# COMMAND ----------

print model.stages[1].bestModel.toDebugString

# COMMAND ----------

importance = zip(model.stages[1].bestModel.featureImportances.toArray(), cols)

# COMMAND ----------

print importance

# COMMAND ----------

temp = sc.parallelize(importance).map(lambda f: [float(f[0]), f[1]])
display(sqlContext.createDataFrame(temp, ["importance", "feature"]).orderBy(f.col("importance").desc()))

# COMMAND ----------

result = model.transform(withGeo)

display(result.select("record", "unpaid_balance", "prediction"))

# COMMAND ----------

result.createGlobalTempView("mortgage_data_predictions")