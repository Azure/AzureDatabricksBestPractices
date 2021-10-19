# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Keras Lab: Boston Housing Dataset
# MAGIC 
# MAGIC Let's build a Keras model to predict the house price on the [Boston Housing dataset](http://scikit-learn.org/stable/modules/generated/sklearn.datasets.load_boston.html).
# MAGIC 
# MAGIC Description of the dataset from François Chollet's book: 
# MAGIC 
# MAGIC >>> You’ll attempt to predict the median price of homes in a given Boston suburb in the mid-1970s, given data points about the suburb at the time, such as the crime rate, the local property tax rate, and so on. 
# MAGIC 
# MAGIC >>> You have 404 training samples and 102 test samples, each with 13 numerical features, such as per capita crime rate, average number of rooms per dwelling, accessibility to highways, and so on. And each feature in the input data (for example, the crime rate) has a different scale. For instance, some values are proportions, which take values between 0 and 1.The targets are the median values of owner-occupied homes, in thousands of dollars.
# MAGIC 
# MAGIC >>> The prices are typically between $10,000 and $50,000. If that sounds cheap, rememberthat this was the mid-1970s, and these prices aren’t adjusted for inflation.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Build and evaluate your first Keras model!

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.datasets import load_boston
import numpy as np
np.random.seed(0)

boston_housing = load_boston()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(boston_housing.data,
                                                        boston_housing.target,
                                                        test_size=0.2,
                                                        random_state=1)

print(boston_housing.DESCR)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the distribution of our features.

# COMMAND ----------

import pandas as pd

xTrainDF = pd.DataFrame(X_train, columns=boston_housing.feature_names)

print(xTrainDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keras Neural Network Life Cycle Model
# MAGIC 
# MAGIC ![Life Cycle](https://brookewenig.github.io/img/DL/Life-Cycle-for-Neural-Network-Models-in-Keras.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Define a Network
# MAGIC 
# MAGIC We need to specify our [dense layers](https://keras.io/layers/core/#dense).
# MAGIC 
# MAGIC The first layer should have 50 units, the second layer, 20 units, and the last layer 1 unit. For all of the layers, make the activation function `relu` except for the last layer, as that activation function should be `linear`.

# COMMAND ----------

# TODO
import tensorflow as tf
tf.set_random_seed(42) # For reproducibility
from keras.models import Sequential
from keras.layers import Dense

model = <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question
# MAGIC 
# MAGIC If you did the previous cell correctly, you should see there are 700 parameters for the first layer. Why are there 700?
# MAGIC 
# MAGIC **HINT**: Add in `use_bias=False` in the Dense layer, and you should see a difference in the number of parameters (don't forget to set this back to `True` before moving on)

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Compile a Network
# MAGIC 
# MAGIC To [compile](https://keras.io/getting-started/sequential-model-guide/#compilation) the network, we need to specify the loss function, which optimizer to use, and a metric to evaluate how well the model is performing.
# MAGIC 
# MAGIC Use `mse` as the loss function, `adam` as the optimizer, and `mse` as the evaluation metric.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Fit a Network
# MAGIC 
# MAGIC Now we are going to [fit](https://keras.io/getting-started/sequential-model-guide/#training) our model to our training data. Set `epochs` to 100 and `batch_size` to 32, `verbose` to 2.

# COMMAND ----------

# TODO
history = <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC Visualize model loss

# COMMAND ----------

import matplotlib.pyplot as plt

def viewModelLoss():
  plt.clf()
  plt.plot(history.history['loss'])
  plt.title('model loss')
  plt.ylabel('loss')
  plt.xlabel('epoch')
  display(plt.show())
viewModelLoss()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Evaluate Network

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Make Predictions

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC Wahoo! You successfully just built your first neural network in Keras! 

# COMMAND ----------

# MAGIC %md
# MAGIC ## BONUS:
# MAGIC Try around with changing some hyperparameters. See what happens if you increase the number of layers, or change the optimizer, etc. What about normalizing the data??
# MAGIC 
# MAGIC If you have time, how about building a baseline model to compare against?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>