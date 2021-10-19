# Databricks notebook source
# MAGIC %md
# MAGIC ![Credit Card ML](/files/img/train_model.png)

# COMMAND ----------

# MAGIC %md #Ingest Data

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.sql("show tables"))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(spark.sql("show tables"))

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TEMPORARY VIEW tmp_amazon AS
# MAGIC select 
# MAGIC  rating, review, time, brand, title
# MAGIC from amazon 
# MAGIC where time < ((select max(time) from amazon) - 86400); --exclude the last day of data
# MAGIC 
# MAGIC CACHE TABLE tmp_amazon;

# COMMAND ----------

# MAGIC %md #Explore Data

# COMMAND ----------

# MAGIC %sql select * from tmp_amazon limit 10

# COMMAND ----------

# MAGIC %sql select brand, count(*) as count from tmp_amazon  group by brand order by count(*) desc limit 10

# COMMAND ----------

# MAGIC %sql select count(*) from tmp_amazon

# COMMAND ----------

# DBTITLE 1,Aggregate Amazon Product Ratings
# MAGIC %sql 
# MAGIC select rating, count(1) as count 
# MAGIC from tmp_amazon 
# MAGIC group by rating 
# MAGIC order by rating

# COMMAND ----------

# MAGIC %md #Train Model

# COMMAND ----------

# DBTITLE 1,Split Data into Training & Test Datasets
splitSample = spark.table('tmp_amazon').randomSplit([.70,.30], 123)
trainingData = splitSample[0]
testData = splitSample[1]

# COMMAND ----------

# DBTITLE 1,Define ML Pipeline with Logistic Regression
from pyspark.ml import *
from pyspark.ml.feature import *
from pyspark.ml.classification import *
from pyspark.ml.tuning import *
from pyspark.ml.evaluation import *
from pyspark.ml.regression import *

bin = Binarizer(inputCol = "rating", outputCol = "label", threshold = 4.5) # Positive reviews > 4.5 threshold
tok = Tokenizer(inputCol = "review", outputCol = "words")
hashTF = HashingTF(inputCol = tok.getOutputCol(), numFeatures = 10000, outputCol = "features")
lr = LogisticRegression(maxIter = 10, regParam = 0.0001, elasticNetParam = 1.0)
pipeline = Pipeline(stages = [bin, tok, hashTF, lr])

# COMMAND ----------

# DBTITLE 1,Fit the Pipeline to the Training Data
model = pipeline.fit(trainingData)

# COMMAND ----------

# MAGIC %md #Evaluate Model

# COMMAND ----------

# DBTITLE 1,Score the Holdout (Test) Dataset
predictions = model.transform(testData)
predictions.createOrReplaceTempView('tmp_predictions') #make dataframe available for SQL access

# COMMAND ----------

# MAGIC %sql select * from tmp_predictions

# COMMAND ----------

# DBTITLE 1,View Prediction of Reviews that include the word "return"
# MAGIC %sql 
# MAGIC select label, prediction, probability, review 
# MAGIC from tmp_predictions 
# MAGIC where review like "%return%"

# COMMAND ----------

# MAGIC %md #Serialize the Model to be Served Later

# COMMAND ----------

# DBTITLE 1,Persist the Model
model.write().overwrite().save("/mnt/joewiden/amazonRevML")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/joewiden/amazonRevML/stages/

# COMMAND ----------

