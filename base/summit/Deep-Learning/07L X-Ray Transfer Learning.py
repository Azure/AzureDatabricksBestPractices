# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # X-Ray Vision with Deep Learning Pipelines
# MAGIC 
# MAGIC In this notebook, we will use Transfer Learning on images of chest x-rays to predict if a patient has pneumonia (bacterial or viral) or is normal. Using the Databricks ML Runtime, we can accomplish this task in under 50 lines of code!
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Build a model with nearly perfect accuracy predicting if a patient has pneumonia or not using transfer learning
# MAGIC  
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **NOTE**: The normal and pneumonia images are located in different directories, so we will have to union them together. We will adjust the test dataset to be roughly 50/50 as well because the test dataset does not contain the true distribution of folks that have pneumonia.
# MAGIC 
# MAGIC We are not using sampleBy here, because the readImages does not push the sample filter down to source.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom Setup"

# COMMAND ----------

from pyspark.ml.image import ImageSchema
from pyspark.sql.functions import lit
import random

normal_train = (ImageSchema.readImages("/mnt/training/chest-xray/train/normal/", sampleRatio=.05, seed=42) 
                .withColumn("label", lit(0)))

normal_test = (ImageSchema.readImages("/mnt/training/chest-xray/test/normal/", sampleRatio=.1, seed=42) 
               .withColumn("label", lit(0)))

pneumonia_train = (ImageSchema.readImages("/mnt/training/chest-xray/train/pneumonia/", sampleRatio=.01, seed=42)
                   .withColumn("label", lit(1)))

pneumonia_test = (ImageSchema.readImages("/mnt/training/chest-xray/test/pneumonia/", sampleRatio=.04, seed=42)
                  .withColumn("label", lit(1)))

train_df = normal_train.unionAll(pneumonia_train).cache()
test_df = normal_test.unionAll(pneumonia_test).cache()

print(f"Number of records in the training set: {train_df.count()}")
print(f"Number of records in the test set: {test_df.count()}")
print(f"Fraction pneumonia in the training set: {pneumonia_train.count()/(normal_train.count()+pneumonia_train.count())}")
print(f"Fraction pneumonia in the test set: {pneumonia_test.count()/(normal_test.count()+pneumonia_test.count())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize the images

# COMMAND ----------

display(train_df.sample(.10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the MLlib Pipeline
# MAGIC 
# MAGIC We are going to use [DeepImageFeaturizer](https://github.com/databricks/spark-deep-learning#transfer-learning) with the `InceptionV3` model. It will apply all layers of the neural network, but remove the last layer. We will re-train the last layer using a Logistic Regression from SparkML.
# MAGIC 
# MAGIC Train the Logistic Regression model for 30 iterations, with a `regParam`=0.10 and `elasticNetParam`=0.3.

# COMMAND ----------

# TODO
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from sparkdl import DeepImageFeaturizer

featurizer = <FILL_IN>
lr = <FILL_IN>
p = <FILL_IN>

p_model = p.fit(<FILL_IN>)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the model
# MAGIC 
# MAGIC From above, we've established our baseline accuracy if we do always predict `normal` in the test set should be 50%. Let's see how we did in comparison!

# COMMAND ----------

# TODO
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

pred_df = <FILL_IN>
evaluator = <FILL_IN>
accuracy = round(<FILL_IN>, 2)
print(f"Predicted the diagnosis of {test_df.count()} patients with an accuracy of {accuracy}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Predictions
# MAGIC 
# MAGIC Wow! That's incredible!!
# MAGIC 
# MAGIC Let's take a look at our predictions, and see which images we were most confident about.

# COMMAND ----------

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col

get_probability = udf(lambda v: float(v.array[1]), DoubleType())

display(pred_df.select("image", "label", "prediction", get_probability(col("probability")).alias("p1_prob")))


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>