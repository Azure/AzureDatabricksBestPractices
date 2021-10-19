# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Linear Regression II Lab
# MAGIC 
# MAGIC Alright! We're making progress. Still not a great RMSE or R2, but better than the baseline or just using a single feature.
# MAGIC 
# MAGIC In the lab, you will see how to improve our performance even more.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Use RFormula to simplify the process of using StringIndexer, OneHotEncoderEstimator, and VectorAssembler
# MAGIC  - Transform the price into log(price), predict, and exponentiate the result for a lower RMSE

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

filePath = "mnt/training/airbnb/sf-listings/sf-listings-2018-12-06-clean.parquet/"
airbnbDF = spark.read.parquet(filePath)
(trainDF, testDF) = airbnbDF.randomSplit([.8, .2], seed=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## RFormula
# MAGIC The StringIndexer, OneHotEncoderEstimator, and VectorAssembler is a little bit verbose to use.
# MAGIC Convert the String Indexer, OHE, and VectorAssembler code to use `RFormula` instead, and verify that you get the same result 
# MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RFormula)/
# MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.RFormula).
# MAGIC 
# MAGIC With RFormula, if you have any columns of type String, it treats it as a categorical and one hot encodes it for us. Otherwise, it leaves as it is. Then it combines all of the features into a single vector, called `features`, and that is what we are going to build our model with. 
# MAGIC 
# MAGIC It effectively does `StringIndex` and `OneHotEncoderEstimator` to all of our categorical fields, and then puts the result with our numeric fields in a `VectorAssembler` manner.

# COMMAND ----------

# TODO
from pyspark.ml import Pipeline
from pyspark.ml.feature import RFormula
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

rFormula = RFormula(<FILL_IN>)
lr = <FILL_IN>
pipeline = Pipeline(<FILL_IN>)
pipelineModel = pipeline.fit(<FILL_IN>)
predDF = pipelineModel.transform(<FILL_IN>)

regressionEvaluator = RegressionEvaluator(<FILL_IN>)

rmse = regressionEvaluator.setMetricName("rmse").evaluate(predDF)
r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
print(f"RMSE is {rmse}")
print(f"R2 is {r2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Scale
# MAGIC 
# MAGIC Now that we have verified we get the same result using RFormula as above, we are going to improve upon our model. If you recall, our price dependent variable appears to be log-normally distributed, so we are going to try to predict it on the log scale.
# MAGIC 
# MAGIC Let's convert our price to be on log scale, and have the linear regression model predict the log price

# COMMAND ----------

from pyspark.sql.functions import log

display(trainDF.select(log("price")))

# COMMAND ----------

# TODO
from pyspark.sql.functions import *

logTrainDF = <FILL_IN>
logTestDF = <FILL_IN>

rFormula = RFormula(<FILL_IN>) # Look at handleInvalid
lr.setLabelCol(<FILL_IN>)
pipeline = Pipeline(stages = [rFormula, lr])
pipelineModel = pipeline.fit(logTrainDF)
predDF = pipelineModel.transform(logTestDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exponentiate
# MAGIC 
# MAGIC In order to interpret our RMSE, we need to convert our predictions back from logarithmic scale.

# COMMAND ----------

# TODO
expDF = <FILL_IN>

rmse = regressionEvaluator.setMetricName("rmse").evaluate(expDF)
r2 = regressionEvaluator.setMetricName("r2").evaluate(expDF)
print(f"RMSE is {rmse}")
print(f"R2 is {r2}")

# COMMAND ----------

# MAGIC %md
# MAGIC Nice job! You have increased the R2 by more than 10x the baseline model, and have dropped the RMSE significantly.
# MAGIC 
# MAGIC In the next few notebooks, we will see how we can reduce the RMSE even more.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>