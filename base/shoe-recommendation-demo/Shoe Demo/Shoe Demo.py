# Databricks notebook source
# MAGIC %md ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Building a Recommendation Engine for Shoes

# COMMAND ----------

# MAGIC %run ./helpers/prepare_keys

# COMMAND ----------

# MAGIC %md ####We first need to create a Recommendation Engine

# COMMAND ----------

# MAGIC %md ### Locality Sensitive Hashing (LSH)
# MAGIC ![image](https://s3-us-west-2.amazonaws.com/databricks-mllib/demo-shoes/presentation_imgs/uber-lsh-og-widest.png)

# COMMAND ----------

# MAGIC %md ####Lets take a look at the current model (v1.1) that uses LSH

# COMMAND ----------

sample_rows.first()["img_array"].toArray().shape
def run_lsh(input_col, candidates, key):
  from pyspark.ml.feature import BucketedRandomProjectionLSH
  
  lsh = BucketedRandomProjectionLSH(inputCol=input_col, outputCol="hashes", bucketLength=2.0, numHashTables=3)
  model = lsh.fit(candidates)

  nn = model.approxNearestNeighbors(candidates, key, numNearestNeighbors=10)
  
  displayTopK(nn)

# COMMAND ----------

run_lsh(input_col="img_array", candidates=image_data, key=arr_key)

# COMMAND ----------

# MAGIC %md ####Using deep learning embeddings
# MAGIC ![image](https://s3-us-west-2.amazonaws.com/databricks-mllib/demo-shoes/presentation_imgs/deep_embedding.png)

# COMMAND ----------

# MAGIC %md ####Iterating on the model using deep image featurizer to combine Machine Learning and Deep Learning

# COMMAND ----------

from sparkdl.transformers.named_image import DeepImageFeaturizer

featurizer = DeepImageFeaturizer(inputCol="image", 
                                 outputCol="features", 
                                 modelName="InceptionV3")

features_ = featurizer.transform(image_data)

# COMMAND ----------

image_data.select("image").take(2)

# COMMAND ----------

run_lsh(input_col="features", candidates=features, key=deep_key)

# COMMAND ----------

run_lsh(input_col="features", candidates=features, key=deep_key2)

# COMMAND ----------

