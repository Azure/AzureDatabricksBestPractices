# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Transfer Learning
# MAGIC 
# MAGIC [Deep Learning Pipelines](https://github.com/databricks/spark-deep-learning) provides utilities to perform transfer learning on images, which is one of the fastest (code and run-time-wise) ways to start using deep learning. Using Deep Learning Pipelines, it can be done in just several lines of code.
# MAGIC 
# MAGIC The **idea** behind transfer learning is to take knowledge from one model doing some task, and transfer it to build another model doing a similar task.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Perform transfer learning to create a cat vs dog classifier

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://files.training.databricks.com/images/mllib_dpl-1.jpg" height="500" width="900" alt="Deep Learning & Transfering Learning" style=>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start by reading in our directory of cat and dog images. We will give cats a label of 0, and dogs a label of 1. We will then do an 80-20 train-test split.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom Setup"

# COMMAND ----------

from pyspark.ml.image import ImageSchema
from pyspark.sql.functions import lit
from sparkdl.image import imageIO

img_dir = 'dbfs:/mnt/training/dl/img'
cats_df = ImageSchema.readImages(img_dir + "/cats/*.jpg").withColumn("label", lit(0))
dogs_df = ImageSchema.readImages(img_dir + "/dogs/*.jpg").withColumn("label", lit(1))

cats_train, cats_test = cats_df.randomSplit([.8, .2], seed=42)
dogs_train, dogs_test = dogs_df.randomSplit([.8, .2], seed=42)

train_df = cats_train.unionAll(dogs_train).cache()
test_df = cats_test.unionAll(dogs_test).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how few images we have in our training data. Will our neural network be able to distinguish between cats and dogs?

# COMMAND ----------

display(train_df.select("image", "label"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the MLlib Pipeline
# MAGIC 
# MAGIC Now, instead of using `DeepImagePredictor`, we are going to use [DeepImageFeaturizer](https://github.com/databricks/spark-deep-learning#transfer-learning). It will apply all layers of the neural network, but remove the last layer. We will re-train the last layer using a SparkML algorithm (such as [LogisticRegression](https://github.com/databricks/spark-deep-learning#transfer-learning)).

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from sparkdl import DeepImageFeaturizer 

featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
lr = LogisticRegression(maxIter=20, regParam=0.05, elasticNetParam=0.3, labelCol="label")
p = Pipeline(stages=[featurizer, lr])

p_model = p.fit(train_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the Accuracy

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

pred_df = p_model.transform(test_df).cache()
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(pred_df.select("prediction", "label"))*100) + "%")

# COMMAND ----------

display(pred_df.select("image", "label", "probability"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Transfer Learning
# MAGIC 
# MAGIC Wow, with just 5 examples of each class, it was able to corectly classify the unseen cat and dog images!
# MAGIC 
# MAGIC Deep Learning Pipelines allows you to retrain the final layer of your neural network. However, if your task is quite different from the images used to train VGG16, you might want to retrain a few more layers. 
# MAGIC 
# MAGIC To accomplish this, Keras allows you to specify which layers you want to retrain (did you notice the trainable parameters section in model.summary?)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>