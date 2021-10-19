# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %sh 
# MAGIC sudo apt-get -y install wget
# MAGIC wget -P /tmp https://github.com/dmlc/xgboost/files/2161553/sparkxgb.zip

# COMMAND ----------

dbutils.fs.cp("file:/tmp/sparkxgb.zip", "/FileStore/mbhide/sparkxgb.zip")

# COMMAND ----------

# MAGIC %sh ls -l /dbfs/user/

# COMMAND ----------

import os
import pyspark

from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

# COMMAND ----------

sc.addPyFile("/dbfs/FileStore/mbhide/sparkxgb.zip")

# COMMAND ----------

import numpy as np
import os
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV


from sparkxgb import XGBoostEstimator

# COMMAND ----------

df_raw = spark\
  .read\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .csv("/FileStore/tables/train.csv")

df_raw.show(10)

# COMMAND ----------

df = df_raw.na.fill(0)
trainDF, testDF = df.randomSplit([0.8, 0.2], seed=24)

sexIndexer = StringIndexer()\
  .setInputCol("Sex")\
  .setOutputCol("SexIndex")\
  .setHandleInvalid("keep")
    
cabinIndexer = StringIndexer()\
  .setInputCol("Cabin")\
  .setOutputCol("CabinIndex")\
  .setHandleInvalid("keep")
    
embarkedIndexer = StringIndexer()\
  .setInputCol("Embarked")\
  .setOutputCol("EmbarkedIndex")\
  .setHandleInvalid("keep")

vectorAssembler = VectorAssembler()\
  .setInputCols(["Pclass", "SexIndex", "Age", "SibSp", "Parch", "Fare", "CabinIndex", "EmbarkedIndex"])\
  .setOutputCol("features")

pipeline = Pipeline().setStages([sexIndexer, cabinIndexer, embarkedIndexer, vectorAssembler])
df_vectorized = pipeline.fit(df).transform(df)
df_vectorized.show(10)

# COMMAND ----------

xgb = XGBoostEstimator(
    featuresCol="features", 
    labelCol="Survived", 
    predictionCol="prediction"
)

# COMMAND ----------

pipeline = Pipeline().setStages([sexIndexer, cabinIndexer, embarkedIndexer, vectorAssembler, xgb])

# COMMAND ----------

model = pipeline.fit(trainDF)

# COMMAND ----------

model.transform(testDF).select(col("PassengerId"), col("Survived"), col("prediction")).show()

# COMMAND ----------

