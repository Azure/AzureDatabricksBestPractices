# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Linear Regression Lab
# MAGIC 
# MAGIC In the previous lesson, we predicted price using just one variable: bedrooms. Now, we want to predict price given a few other features.
# MAGIC 
# MAGIC Steps:
# MAGIC 0. Use the features: `bedrooms`, `bathrooms`, `bathrooms_na`, `minimum_nights`, and `number_of_reviews` as input to your VectorAssembler.
# MAGIC 0. Build a Linear Regression Model
# MAGIC 0. Evaluate the `RMSE` and the `R2`.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Build a linear regression model with multiple features
# MAGIC  - Compute various metrics to evaluate goodness of fit

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

filePath = "mnt/training/airbnb/sf-listings/sf-listings-2018-12-06-clean.parquet/"
airbnbDF = spark.read.parquet(filePath)
(trainDF, testDF) = airbnbDF.randomSplit([.8, .2], seed=42)

# COMMAND ----------

# # TODO

# from pyspark.ml.feature import VectorAssembler
# from pyspark.ml.evaluation import RegressionEvaluator
# from pyspark.ml.regression import LinearRegression

# vecAssembler = # FILL IN

# lrModel = # FILL IN

# predDF = # FILL IN

# regressionEvaluator = RegressionEvaluator(predictionCol='prediction', labelCol='price', metricName='rmse')
# rmse = # FILL IN
# r2 = # FILL IN
# print(f"RMSE is {rmse}")
# print(f"R2 is {r2}")

# COMMAND ----------

# ANSWER
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression

vecAssembler = VectorAssembler(inputCols = ["bedrooms", "bathrooms", "bathrooms_na", "minimum_nights", "number_of_reviews"], outputCol = "features")

vecTrainDF = vecAssembler.transform(trainDF)
vecTestDF = vecAssembler.transform(testDF)

lrModel = LinearRegression(featuresCol = "features", labelCol = "price").fit(vecTrainDF)

predDF = lrModel.transform(vecTestDF)

regressionEvaluator = RegressionEvaluator(predictionCol='prediction', labelCol='price', metricName='rmse')
rmse = regressionEvaluator.evaluate(predDF)
r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
print(f"RMSE is {rmse}")
print(f"R2 is {r2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Yikes! We built a pretty bad model. In the next notebook, we will see how we can further improve upon our model.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>