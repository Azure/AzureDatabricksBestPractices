# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Exploration
# MAGIC 
# MAGIC In this notebook, we will use the dataset we cleansed in the previous lab to do some Exploratory Data Analysis (EDA).
# MAGIC 
# MAGIC This will help us better understand our data to make a better model.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Identify log-normal distributions
# MAGIC  - Build a baseline model and evaluate

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

filePath = "mnt/training/airbnb/sf-listings/sf-listings-2018-12-06-clean.parquet/"
airbnbDF = spark.read.parquet(filePath)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's make sure we don't have any null values in our DataFrame

# COMMAND ----------

recordCount = airbnbDF.count()
noNullsRecordCount = airbnbDF.na.drop().count()

print(f"We have {recordCount - noNullsRecordCount} records that contain null values.")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's make a histogram of the price column to explore it (change the number of bins to 300).  

# COMMAND ----------

display(airbnbDF.select("price"))

# COMMAND ----------

# MAGIC %md
# MAGIC Is this a <a href="https://en.wikipedia.org/wiki/Log-normal_distribution" target="_blank">Log Normal</a> distribution? Take the `log` of price and check the histogram. Keep this in mind for later :).

# COMMAND ----------

# TODO

display(<FILL_IN>)

# COMMAND ----------

# MAGIC %md
# MAGIC Now take a look at how `price` depends on some of the variables:
# MAGIC * Plot `price` vs `bedrooms`
# MAGIC * Plot `price` vs `accomodates`
# MAGIC 
# MAGIC Make sure to change the aggregation to `AVG`.

# COMMAND ----------

display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the distribution of some of our categorical features

# COMMAND ----------

display(airbnbDF.groupBy("room_type").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Which neighborhoods have the highest number of rentals? Display the neighbourhoods and their associated count in descending order.

# COMMAND ----------

# TODO
display(<FILL_IN>)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### How much does the price depend on the location?

# COMMAND ----------

airbnbDF.createOrReplaceTempView("airbnbDF")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import col
# MAGIC 
# MAGIC airbnbDF = spark.table("airbnbDF")
# MAGIC 
# MAGIC lat_long_price_values = airbnbDF.select(col("latitude"),col("longitude"),col("price")/600).collect()
# MAGIC 
# MAGIC lat_long_price_strings = [
# MAGIC   "[{}, {}, {}]".format(lat, long, price) 
# MAGIC   for lat, long, price in lat_long_price_values
# MAGIC ]
# MAGIC 
# MAGIC v = ",\n".join(lat_long_price_strings)
# MAGIC 
# MAGIC displayHTML("""
# MAGIC <html>
# MAGIC <head>
# MAGIC  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.3.1/dist/leaflet.css"
# MAGIC    integrity="sha512-Rksm5RenBEKSKFjgI3a41vrjkw4EVPlJ3+OiI65vTjIdo9brlAacEuKOiQ5OFh7cOI1bkDwLqdLw3Zg0cRJAAQ=="
# MAGIC    crossorigin=""/>
# MAGIC  <script src="https://unpkg.com/leaflet@1.3.1/dist/leaflet.js"
# MAGIC    integrity="sha512-/Nsx9X4HebavoBvEBuyp3I7od5tA0UzAxs+j83KgC8PU0kgB4XiK4Lfe4y4cgBtaRJQEIFCW+oC506aPT2L1zw=="
# MAGIC    crossorigin=""></script>
# MAGIC  <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>
# MAGIC </head>
# MAGIC <body>
# MAGIC     <div id="mapid" style="width:700px; height:500px"></div>
# MAGIC   <script>
# MAGIC   var mymap = L.map('mapid').setView([37.7587,-122.4486], 12);
# MAGIC   var tiles = L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
# MAGIC     attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors',
# MAGIC }).addTo(mymap);
# MAGIC   var heat = L.heatLayer([""" + v + """], {radius: 25}).addTo(mymap);
# MAGIC   </script>
# MAGIC   </body>
# MAGIC   </html>
# MAGIC """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline Model
# MAGIC 
# MAGIC Before we build any Machine Learning models, we want to build a baseline model to compare to. We also want to determine a metric to evaluate our model. Let's use RMSE here.
# MAGIC 
# MAGIC For this dataset, let's build a baseline model that always predict the average price and one that always predicts the [median](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.approxQuantile) price, and see how we do.
# MAGIC 
# MAGIC 0. Start by extracting the average and median price, and store them in the variables `avgPrice` and `medianPrice`, respectively
# MAGIC 0. Then add two new columns: `avgPrediction` and `medianPrediction` with the average and median price, respectively. Call the resulting DataFrame `predDF`. 
# MAGIC 
# MAGIC Some useful functions:
# MAGIC * avg() [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.avg)/
# MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.functions$)
# MAGIC * col() [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.col)/
# MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.functions$)
# MAGIC * lit() [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.lit)/
# MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.functions$)
# MAGIC * approxQuantile() [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.approxQuantile)/
# MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.DataFrameStatFunctions) [**HINT**: There is no median function, so you will need to use approxQuantile]
# MAGIC * withColumn() [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.withColumn)/
# MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset)

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate model
# MAGIC 
# MAGIC We are going to use SparkML's `RegressionEvaluator` to compute the RMSE for our average price and median price predictions 
# MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.RegressionEvaluator)/
# MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.evaluation.RegressionEvaluator).

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

regressionMeanEvaluator = RegressionEvaluator(predictionCol="avgPrediction", labelCol="price", metricName="rmse")

print(f"The RMSE for predicting the average price is: {regressionMeanEvaluator.evaluate(predDF)}")

regressionMedianEvaluator = RegressionEvaluator(predictionCol="medianPrediction", labelCol="price", metricName="rmse")
  
print(f"The RMSE for predicting the median price is: {regressionMedianEvaluator.evaluate(predDF)}")

# COMMAND ----------

# MAGIC %md
# MAGIC Wow! We can see that always predicting median or mean doesn't do too well for our dataset. Let's see if we can improve this with a machine learning model!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>