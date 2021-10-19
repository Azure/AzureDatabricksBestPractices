# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Keras
# MAGIC 
# MAGIC In this notebook, we will build upon the concepts introduce the in previous lab to build a neural network that is more powerful than a simple linear regression model!
# MAGIC 
# MAGIC We will use the California Housing Dataset.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC   - Will modify these parameters for increased model performance:
# MAGIC     - Activation functions
# MAGIC     - Loss functions
# MAGIC     - Optimizer
# MAGIC     - Batch Size

# COMMAND ----------

from sklearn.datasets.california_housing import fetch_california_housing
from sklearn.model_selection import train_test_split
import numpy as np
np.random.seed(0)

cal_housing = fetch_california_housing()

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(cal_housing.data,
                                                        cal_housing.target,
                                                        test_size=0.2,
                                                        random_state=1)

print(cal_housing.DESCR)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Recall from Last Lab
# MAGIC 
# MAGIC ##### Steps to build a Keras model
# MAGIC <img style="width:20%" src="https://files.training.databricks.com/images/5_cycle.jpg" >

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Network
# MAGIC 
# MAGIC Let's not just reinvent linear regression. Let's build a model, but with multiple layers using the [Sequential model](https://keras.io/getting-started/sequential-model-guide/) from Keras.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/Neural_network.svg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Activation Function
# MAGIC 
# MAGIC If we keep the activation as linear, then we aren't utilizing the power of neural networks!! The power of neural networks derives from the non-linear combinations of linear functions.
# MAGIC 
# MAGIC **RECAP:** So what are our options for [activation functions](http://cs231n.github.io/neural-networks-1/#actfun)? 

# COMMAND ----------

import tensorflow as tf
tf.set_random_seed(42) # For reproducibility

from keras.models import Sequential
from keras.layers import Dense

model = Sequential()

# Input layer
model.add(Dense(20, input_dim=8, activation='relu')) 

# Automatically infers the input_dim based on the layer before it
model.add(Dense(20, activation='relu')) 

# Output layer
model.add(Dense(1, activation='linear')) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Alternative Keras Model Syntax

# COMMAND ----------

def build_model():
  return Sequential([Dense(20, input_dim=8, activation='relu'),
                    Dense(20, activation='relu'),
                    Dense(1, activation='linear')]) # Keep the last layer as linear because this is a regression problem

# COMMAND ----------

# MAGIC %md
# MAGIC We can check the model definition by calling `.summary()`

# COMMAND ----------

model = build_model()
model.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Loss Functions + Metrics
# MAGIC 
# MAGIC In Keras, the *loss function* is the function for our optimizer to minimize. *[Metrics](https://keras.io/metrics/)* are similar to a loss function, except that the results from evaluating a metric are not used when training the model.
# MAGIC 
# MAGIC **Recap:** Which loss functions should we use for regression? Classification?

# COMMAND ----------

from keras import metrics
from keras import losses

loss = "mse" # Or loss = losses.mse
metrics = ["mae", "mse"] # Or metrics = [metrics.mae, metrics.mse]

model.compile(optimizer="sgd", loss=loss, metrics=metrics)
model.fit(X_train, y_train, epochs=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Optimizer
# MAGIC 
# MAGIC WOW! We got a lot of NANs! Let's try this again, but using the Adam optimizer. There are a lot of optimizers out there, and here is a [great blog post](http://ruder.io/optimizing-gradient-descent/) illustrating the various optimizers.
# MAGIC 
# MAGIC When in doubt, the Adam optimizer does a very good job. If you want to adjust any of the hyperparameters, you will need to import the optimizer from `optimizers` instead of passing in the name as a string.

# COMMAND ----------

# Configure custom optimizer: 
from keras import optimizers

model = build_model()
optimizer=optimizers.Adam(lr=0.001)

model.compile(optimizer=optimizer, loss=loss, metrics=metrics)
history = model.fit(X_train, y_train, epochs=20)

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
# MAGIC ## 4. Batch Size
# MAGIC 
# MAGIC Let's set our `batch_size` (how much data to be processed simultaneously by the model) to 64, and increase our `epochs` to 20. Mini-batches are often a power of 2, to facilitate memory allocation on GPU (typically between 8 and 128).
# MAGIC 
# MAGIC 
# MAGIC Also, if you don't want to see all of the intermediate values print out, you can set the `verbose` parameter: 0 = silent, 1 = progress bar, 2 = one line per epoch (defaults to 1)

# COMMAND ----------

model = build_model()
model.compile(optimizer=optimizer, loss=loss, metrics=metrics)
history = model.fit(X_train, y_train, epochs=20, batch_size=64, verbose=2)

# COMMAND ----------

# Change the batch size to 32, and notice how choppy it is!


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>