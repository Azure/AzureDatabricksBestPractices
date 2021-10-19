# Databricks notebook source
# MAGIC %md #Identifying Suspicious Behavior in Security Footage  
# MAGIC 
# MAGIC ![image](/files/mnt/raela/video_splash.png)

# COMMAND ----------

# MAGIC %run ./display_vid

# COMMAND ----------

# MAGIC %md ## Load training data

# COMMAND ----------

# Read in label data
labels_df = spark.read.parquet("/mnt/raela/cctv_labels/")

# Join with training dataset
featureDF = spark.read.parquet("/mnt/raela/cctv_features2")
train = featureDF.join(labels_df, featureDF.filePath == labels_df.filePath).select("features", "label", featureDF.filePath)

# COMMAND ----------

# MAGIC %md ## Train LogisticRegression model

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

# Fit LogisticRegression Model
lr = LogisticRegression(maxIter=20, regParam=0.05, elasticNetParam=0.3, labelCol="label")
lrModel = lr.fit(train)

# Persist lrModel
#lrModel.save("/mnt/raela/suspiciousModel/")

# COMMAND ----------

# MAGIC %md ## Generate Predictions on Test data

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel

# Load Persisted lrModel
lrModel = LogisticRegressionModel.load("/mnt/raela/suspiciousModel/")

# Load Test Data
featuresTestDF = spark.read.parquet("/mnt/raela/cctv_features_test/")

# Generate predictions on test data
result = lrModel.transform(featuresTestDF)

# COMMAND ----------

# MAGIC %md ## Any suspicious activity in our videos?

# COMMAND ----------

display(result.select("filePath", "probability", "prediction").where("prediction==1"))

# COMMAND ----------

displayImg("/mnt/raela/cctvFrames_test/Fight_OneManDownframe0018.jpg")

# COMMAND ----------

displayVid("/mnt/raela/cctvVideos/mp4/test/Fight_OneManDown.mp4")

# COMMAND ----------

display(result.select("filePath", "probability", "prediction").where("prediction==0"))

# COMMAND ----------

displayImg("/mnt/raela/cctvFrames_test/Browse_WhileWaiting2frame0034.jpg")

# COMMAND ----------

displayVid("/mnt/raela/cctvVideos/mp4/test/Browse_WhileWaiting2.mp4")

# COMMAND ----------

