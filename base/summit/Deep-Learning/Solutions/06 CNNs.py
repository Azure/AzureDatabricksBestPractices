# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Convolutional Neural Networks
# MAGIC 
# MAGIC We will use pre-trained Convolutional Neural Networks (CNNs), trained with the image dataset from [ImageNet](http://www.image-net.org/), to demonstrate two aspects. First, how to explore and classify images. And second, how to use transfer learning with existing trained models (next lab).
# MAGIC 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Analyze popular CNN architectures
# MAGIC  - Apply pre-trained CNNs to images

# COMMAND ----------

# MAGIC %md
# MAGIC ## VGG16
# MAGIC ![vgg16](https://neurohive.io/wp-content/uploads/2018/11/vgg16-neural-network.jpg)
# MAGIC 
# MAGIC We are going to start with the VGG16 model, which was introduced by Simonyan and Zisserman in their 2014 paper [Very Deep Convolutional Networks for Large Scale Image Recognition](https://arxiv.org/abs/1409.1556).
# MAGIC 
# MAGIC Let's start by downloading VGG's weights and model architecture.

# COMMAND ----------

from keras.preprocessing import image
from keras.applications.vgg16 import preprocess_input, decode_predictions, VGG16
import numpy as np
import os

vgg16Model = VGG16(weights='imagenet')

# COMMAND ----------

# MAGIC %md
# MAGIC We can look at the model summary. Look at how many parameters there are! Imagine if you had to train all 138,357,544 parameters from scratch! This is one motivation for re-using existing model weights.
# MAGIC 
# MAGIC **RECAP**: What is a convolution? Max pooling?

# COMMAND ----------

vgg16Model.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC **Question**: What do the input and output shapes represent?

# COMMAND ----------

# MAGIC %md
# MAGIC In Tensorflow, it represents the images in a channels-last manner: (samples, height, width, color_depth)
# MAGIC 
# MAGIC But in other frameworks, such as Theano, the same data would be represented channels-first: (samples, color_depth, height, width)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply pre-trained model
# MAGIC 
# MAGIC We are going to make a helper method to resize our images to be 224 x 224, and output the top 3 classes for a given image.

# COMMAND ----------

def predict_images(images, model):
  for i in images:
    print ('processing image:', i)
    img = image.load_img(i, target_size=(224, 224))
    #convert to numpy array for Keras image formate processing
    x = image.img_to_array(img)
    x = np.expand_dims(x, axis=0)
    x = preprocess_input(x)
    preds = model.predict(x)
    # decode the results into a list of tuples (class, description, probability
    print('Predicted:', decode_predictions(preds, top=3)[0], '\n')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Images
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://files.training.databricks.com/images/pug.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/strawberries.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/rose.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   
# MAGIC </div>
# MAGIC 
# MAGIC Let's make sure the datasets are already mounted.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom Setup"

# COMMAND ----------

img_paths = ["/dbfs/mnt/training/dl/img/pug.jpg", "/dbfs/mnt/training/dl/img/strawberries.jpg", "/dbfs/mnt/training/dl/img/rose.jpg"]
predict_images(img_paths, vgg16Model)

# COMMAND ----------

# MAGIC %md
# MAGIC The network did so well with the pug and strawberry! What happened with the rose? Well, it turns out that `rose` was not one of the 1000 categories that VGG16 had to predict. But it is quite interesting it predicted `sea_anemone` and `vase`.

# COMMAND ----------

# MAGIC %md
# MAGIC You can play around with this with your own images by doing the following:
# MAGIC 
# MAGIC Get a new file: 
# MAGIC 
# MAGIC `%sh wget image_url.jpg`
# MAGIC 
# MAGIC `%fs cp file:/databricks/driver/image_name.jpg yourName/tmp/image_name.jpg `
# MAGIC 
# MAGIC 
# MAGIC OR
# MAGIC 
# MAGIC You can upload this file via the Data UI and read in from the FileStore path (e.g. `/dbfs/FileStore/image_name.jpg`).

# COMMAND ----------

# MAGIC %md
# MAGIC # DeepImagePredictor
# MAGIC 
# MAGIC While it's great and fun to play around with Keras on the driver, what about loading a copy of the model to our workers, and make these predictions in parallel on our Spark cluster using [Deep Learning Pipelines](https://github.com/databricks/spark-deep-learning)! 
# MAGIC 
# MAGIC Deep Learning Pipelines is an open-source project started by Databricks for distributed model inference of deep learning models.
# MAGIC 
# MAGIC **NOTE**: We are running it on a small dataset, and there is quite high overhead of copying the model to our workers. This is useful when your datasets are quite large

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Classify Co-Founders of Databricks
# MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://files.training.databricks.com/images/Ali-Ghodsi-4.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/andy-konwinski-1.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/ionS.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/MateiZ.jpg" height="200" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/patrickW.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC   <img src="https://files.training.databricks.com/images/Reynold-Xin.jpg" height="150" width="150" alt="Databricks Nerds!" style=>
# MAGIC </div>

# COMMAND ----------

from pyspark.ml.image import ImageSchema
from sparkdl.image import imageIO
from sparkdl import DeepImagePredictor

df = ImageSchema.readImages("mnt/training/dl/img/founders/")

predictor = DeepImagePredictor(inputCol="image", outputCol="predicted_labels", modelName="VGG16", decodePredictions=True, topK=5)
predictions_df = predictor.transform(df).cache()
display(predictions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inception V3
# MAGIC 
# MAGIC What happened to Matei's predictions: Chain mail?? Well, he truly is Spark's knight in shining armor!
# MAGIC 
# MAGIC Let's change the model to a more recent architecture, and see what the predictions are now!

# COMMAND ----------

from pyspark.ml.image import ImageSchema
from sparkdl.image import imageIO
from sparkdl import DeepImagePredictor

df = ImageSchema.readImages("mnt/training/dl/img/founders/")

predictor = DeepImagePredictor(inputCol="image", outputCol="predicted_labels", modelName="InceptionV3", decodePredictions=True, topK=5)
predictions_df = predictor.transform(df).cache()
display(predictions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Yikes! These are not the most natural predictions (because ImageNet did not have a `person` category). In the next lab, we will cover how to utilize existing components of the VGG16 architecture, and how to retrain the final classifier.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>