# Databricks notebook source
# MAGIC %md XGBoost + PySpark
# MAGIC * uses v0.80 of xgboost
# MAGIC * must use v0.80 versions of xgboost4j and xgboost4j-spark
# MAGIC * compatible with spark version 2.3

# COMMAND ----------

# MAGIC %sh 
# MAGIC sudo apt-get -y install wget
# MAGIC #wget -P /tmp https://github.com/dmlc/xgboost/files/2161553/sparkxgb.zip (this is xgboost:0.72 version)
# MAGIC wget -P /tmp https://github.com/dmlc/xgboost/files/2375046/sparkxgb.zip

# COMMAND ----------

dbutils.fs.cp("file:/tmp/sparkxgb.zip", "/Users/mbhide/sparkxgb.zip")

# COMMAND ----------

sc.addPyFile("/dbfs/Users/mbhide/sparkxgb.zip")

# COMMAND ----------

df = spark.read.csv("/databricks-datasets/bikeSharing/data-001/hour.csv", header="true", inferSchema="true")
df.cache()
display(df)

# COMMAND ----------

df = df.drop("instant").drop("dteday").drop("casual").drop("registered")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

import os
import pyspark

from pyspark.sql.functions import col

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, VectorIndexer
from sklearn.model_selection import GridSearchCV
from sparkxgb import XGBoostRegressor

# COMMAND ----------

df = df.select([col(c).cast("double").alias(c) for c in df.columns])
df.printSchema()

# COMMAND ----------

train, test = df.randomSplit([0.7, 0.3])
print "We have %d training examples and %d test examples." % (train.count(), test.count())

# COMMAND ----------

display(train.select("hr", "cnt"))

# COMMAND ----------

featuresCols = df.columns
featuresCols.remove('cnt')
# This concatenates all feature columns into a single feature vector in a new column "rawFeatures".
vectorAssembler = VectorAssembler(inputCols=featuresCols, outputCol="rawFeatures")
# This identifies categorical features and indexes them.
vectorIndexer = VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=4)

# COMMAND ----------

# Takes the "features" column and learns to predict "cnt"
xgb = XGBoostRegressor(labelCol="cnt")

# COMMAND ----------

# Define a grid of hyperparameters to test:
#  - maxDepth: max depth of each decision tree in the GBT ensemble
#  - maxIter: iterations, i.e., number of trees in each GBT ensemble
# In this example notebook, we keep these values small.  In practice, to get the highest accuracy, you would likely want to try deeper trees (10 or higher) and more trees in the ensemble (>100).
paramGrid = ParamGridBuilder()\
  .addGrid(xgb.maxDepth, [2, 5, 10])\
  .addGrid(xgb.eta, [0.2, 0.3, 0.4, 0.5])\
  .build()
# We define an evaluation metric.  This tells CrossValidator how well we are doing by comparing the true labels with predictions.
evaluator = RegressionEvaluator(metricName="rmse", labelCol="cnt", predictionCol=xgb.setPredictionCol())
# Declare the CrossValidator, which runs model tuning for us.
cv = CrossValidator(estimator=xgb, evaluator=evaluator, estimatorParamMaps=paramGrid)

pipeline = Pipeline(stages=[vectorAssembler, vectorIndexer, cv])
pipelineModel = pipeline.fit(train)

# COMMAND ----------

predictions = pipelineModel.transform(test)
display(predictions.select("cnt", "prediction", *featuresCols))

# COMMAND ----------

learning_rate = ['0.2', '0.3', '0.4', '0.5']
max_depth = list(range(3,8))
param_grid = dict(learning_rate=learning_rate, max_depth=max_depth)

# COMMAND ----------

grid = GridSearchCV(xgb, param_grid, cv=3, scoring='accuracy')

# COMMAND ----------

grid.fit(trainDF)

# COMMAND ----------

model.evaluation_metrics
model.transform(testDF).select(col("PassengerId"), col("Survived"), col("prediction")).show()

# COMMAND ----------

