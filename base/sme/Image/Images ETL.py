# Databricks notebook source
# MAGIC %md ##Images ETL via Binary File Data Source and model inference with complex return types (DBR/MLR 5.4 & Spark 3.0)
# MAGIC 
# MAGIC --
# MAGIC 
# MAGIC * This notebook was tested in dogfood w/ Runtime 5.4 Conda Beta.

# COMMAND ----------

# MAGIC %md ![](https://user-images.githubusercontent.com/829644/56877594-0488d300-6a04-11e9-9064-5047dfedd913.png)

# COMMAND ----------

# MAGIC %sh ls -R /dbfs/meng/raw-images

# COMMAND ----------

# Read all JPEG images and recognize the date partitions.
# Binary file data source can be used with Structured Streaming.
df = spark.read.format("binaryFile").option("pathGlobFilter", "*.jpeg").load("/meng/raw-images")

# COMMAND ----------

# Binary file data source supports filter pushdown on modificationTime and length.
# The follow job will skip reading files with length >= 7000.
df.filter("length < 7000").show()

# COMMAND ----------

# Save them to a Delta table, so we don't need to deal with many small raw files.
df.write.format("delta").mode("overwrite").saveAsTable("meng.images")
sql("OPTIMIZE meng.images")

# COMMAND ----------

# 5.4 Conda Beta supports notebook-scoped libraries
dbutils.library.installPyPI("tensorflow")
dbutils.library.installPyPI("pillow")
dbutils.library.installPyPI("keras")

# COMMAND ----------

import tensorflow as tf
import io
from PIL import Image
import pandas as pd
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
import numpy as np
from pyspark.sql.types import *

# COMMAND ----------

import keras
from keras.preprocessing import image
from keras.applications.resnet50 import ResNet50, preprocess_input, decode_predictions

# COMMAND ----------

model = ResNet50()
bc_model_weights = sc.broadcast(model.get_weights())

# COMMAND ----------

def preprocess_df(content_batch):
  records = []
  for content in content_batch:
    img = Image.open(io.BytesIO(content)).resize([224, 224])
    x = image.img_to_array(img)
    x = np.expand_dims(x, axis=0)
    x = preprocess_input(x)
    records.append(x)
  return np.vstack(records)

def predict_batch(content_batch):
  model = ResNet50(weights=None)
  model.set_weights(bc_model_weights.value)
  records = preprocess_df(content_batch)
  predictions = model.predict(records)
  labels = [i[0] for i in decode_predictions(predictions, top=1)]
  return pd.DataFrame(columns=['label_id', 'label', 'prob'], data=labels)

# COMMAND ----------

# Test locally and get the result schema.
records = table("meng.images").select('content').limit(4).toPandas()['content']
predictions = predict_batch(records)
result_schema = spark.createDataFrame(predictions).schema

# COMMAND ----------

# In DBR 5.4, Pandas UDF supports complex return types, backported from Spark master branch.
predict_batch_udf = pandas_udf(result_schema, PandasUDFType.SCALAR)(predict_batch)
predictions_df = table("meng.images").select(col("path"), predict_batch_udf(col("content")).alias("prediction"))
display(predictions_df)