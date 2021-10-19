# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Linear Regression
# MAGIC 
# MAGIC Before you attempt to throw a neural network at a problem, you want to establish a __baseline model__. Often, this will be a simple model, such as linear regression. Once we establish a baseline, then we can get started with Deep Learning.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Build a linear regression model using Sklearn and reimplement it in Keras 
# MAGIC  - Modify # of epochs
# MAGIC  - Visualize loss

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start by making a simple array of features, and the label we are trying to predict is y = 2*X + 1.

# COMMAND ----------

import numpy as np

np.set_printoptions(suppress=True)

X = np.arange(-10, 11).reshape((21,1))
y = 2*X + 1

list(zip(X, y))

# COMMAND ----------

import matplotlib.pyplot as plt
plt.plot(X, y, 'ro')
display(plt.show())

# COMMAND ----------

# MAGIC %md
# MAGIC Use Sklearn to establish our baseline (for simplicity, we are using the same dataset for training and testing in this toy example, but we will change that later).

# COMMAND ----------

from sklearn.linear_model import LinearRegression

lr = LinearRegression()

lr.fit(X, y)

# COMMAND ----------

from sklearn.metrics import mean_squared_error

y_pred = lr.predict(X)
mse = mean_squared_error(y, y_pred)
print(mse)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Predictions

# COMMAND ----------

plt.plot(X, y_pred)
display(plt.show())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Keras
# MAGIC 
# MAGIC Now that we have established a baseline model, let's see if we can build a fully-connected neural network that can meet or exceed our linear regression model. A fully-connected neural network is simply a set of matrix multiplications followed by some non-linear function (to be discussed later). 
# MAGIC 
# MAGIC [Keras](https://keras.io/) is a high-level API to build neural networks, that supports the following backends: Tensorflow, Theano, and CNTK. 
# MAGIC 
# MAGIC It was released by François Chollet in 2015, and is now the official high-level API of Tensorflow. Keras has over 250,000 users, and we will primarily use Keras in this course.
# MAGIC 
# MAGIC ##### Steps to build a Keras model
# MAGIC <img style="width:20%" src="https://files.training.databricks.com/images/5_cycle.jpg" >

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Define a N-Layer Network
# MAGIC 
# MAGIC Here, we need to specify the dimensions of our input and output layers. When we say something is an N-layer neural network, we count all of the layers except the input layer. The diagram below demonstrates a 3-layer neural network. 
# MAGIC 
# MAGIC A special case of neural network with no hidden layers and no non-linearities is actually just linear regression :).
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/Neural_network.svg)
# MAGIC 
# MAGIC For the next few labs, we will use the [Sequential model](https://keras.io/getting-started/sequential-model-guide/) from Keras.

# COMMAND ----------

import tensorflow as tf
tf.set_random_seed(42) # For reproducibility
np.random.seed(0)

from keras.models import Sequential
from keras.layers import Dense

# The Sequential model is a linear stack of layers.
model = Sequential()

model.add(Dense(units=1, input_dim=1, activation='linear'))

# COMMAND ----------

# MAGIC %md
# MAGIC We can check the model definition by calling `.summary()`. Note the two parameters - any thoughts on why there are TWO?

# COMMAND ----------

model.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Compile a Network
# MAGIC 
# MAGIC To [compile](https://keras.io/getting-started/sequential-model-guide/#compilation) the network, we need to specify the loss function and which optimizer to use. We'll talk more about optimizers and loss metrics in the next lab.
# MAGIC 
# MAGIC For right now, we will use `mse` (mean squared error) for our loss function, and the `adam` optimizer.

# COMMAND ----------

model.compile(loss='mse',
              optimizer='adam')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Fit a Network
# MAGIC 
# MAGIC Let's [train](https://keras.io/getting-started/sequential-model-guide/#training) our model on X and y.

# COMMAND ----------

model.fit(X, y)

# COMMAND ----------

# MAGIC %md
# MAGIC And take a look at the predictions.

# COMMAND ----------

keras_pred = model.predict(X)
keras_pred

# COMMAND ----------

def kerasPredPlot(keras_pred):
  plt.clf()
  plt.plot(X, y, 'ro', label='True')
  plt.plot(X, keras_pred, 'co', label='Keras Pred')
  plt.legend(numpoints=1)
  display(plt.show())
  
kerasPredPlot(keras_pred)

# COMMAND ----------

# MAGIC %md
# MAGIC What went wrong?? Turns out there a few more hyperparameters we need to set. Let's take a look at [Keras documentation](https://keras.io/models/sequential/).
# MAGIC 
# MAGIC `epochs` specifies how many passes you want over your entire dataset. Let's increase the number of epochs, and look at how the MSE decreases.
# MAGIC 
# MAGIC Here we are capturing the output of model.fit() as it returns a History object, which keeps a record of training loss values and metrics values at successive epochs, as well as validation loss values and validation metrics values (if applicable).

# COMMAND ----------

history = model.fit(X, y, epochs=20) 

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
# MAGIC Let's try increasing the epochs even more.

# COMMAND ----------

history = model.fit(X, y, epochs=4000)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's inspect how our model decreased the loss (MSE) as the number of epochs increases.

# COMMAND ----------

viewModelLoss()

# COMMAND ----------

# MAGIC %md
# MAGIC Extract model weights. Wahoo! We were able to approximate y=2*X + 1 quite well! If we trained for some more epochs, we should be able to approximate this function exactly (at risk of overfitting of course).

# COMMAND ----------

print(model.get_weights())
predicted_w = model.get_weights()[0][0][0]
predicted_b = model.get_weights()[1][0]

print("predicted_w: ", predicted_w)
print("predicted_b: ", predicted_b)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Evaluate Network
# MAGIC 
# MAGIC As mentioned previously, we want to make sure our neural network can beat our benchmark. 

# COMMAND ----------

model.evaluate(X, y) # Prints loss value & metrics values for the model in test mode (both are MSE here)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Make Predictions

# COMMAND ----------

keras_pred1 = model.predict(X)
keras_pred1

# COMMAND ----------

kerasPredPlot(keras_pred1)

# COMMAND ----------

# MAGIC %md
# MAGIC Alright, this was a very simple, contrived example. Let's go ahead and make this a bit more interesting in the next lab!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>