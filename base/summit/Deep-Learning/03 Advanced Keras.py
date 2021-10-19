# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Advanced Keras
# MAGIC 
# MAGIC Congrats on building your first neural network! In this notebook, we will cover even more topics to improve your model building. After you learn the concepts here, you will apply them to the neural network you just created.
# MAGIC 
# MAGIC We will use the California Housing Dataset.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Perform data standardization for better model convergence
# MAGIC  - Create custom metrics
# MAGIC  - Add validation data
# MAGIC  - Generate model checkpointing/callbacks
# MAGIC  - Save and load models

# COMMAND ----------

# MAGIC %run "./Includes/Classroom Setup"

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

# MAGIC %md
# MAGIC Let's take a look at the distribution of our features.

# COMMAND ----------

import pandas as pd

xTrainDF = pd.DataFrame(X_train, columns=cal_housing.feature_names)

print(xTrainDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Standardization
# MAGIC 
# MAGIC Because our features are all on different scales, it's going to be more difficult for our neural network during training. Let's do feature-wise standardization.
# MAGIC 
# MAGIC We are going to use the [StandardScaler](http://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html) from Sklearn, which will remove the mean (zero-mean) and scale to unit variance.
# MAGIC 
# MAGIC $$x' = \frac{x - \bar{x}}{\sigma}$$

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Keras Model

# COMMAND ----------

import tensorflow as tf
tf.set_random_seed(42) # For reproducibility

from keras.models import Sequential
from keras.layers import Dense

model = Sequential([
  Dense(20, input_dim=8, activation='relu'),
  Dense(20, activation='relu'),
  Dense(1, activation='linear')
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Custom Metrics
# MAGIC 
# MAGIC Up until this point, we used MSE as our loss function and metric of choice. But what if we want to use RMSE?

# COMMAND ----------

try:
  model.compile(optimizer="adam", loss="rmse")
  
except ValueError as e:
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC Looks like we can't use it in our loss function. What about the metrics we print out during the evaluation?

# COMMAND ----------

try:
  model.compile(optimizer="adam", loss="mse", metrics=["rmse"])

except ValueError as e:
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC Luckily, Keras allows you to define custom metrics. So, you might implement RMSE as below.

# COMMAND ----------

from keras import backend
 
def rmse(y_true, y_pred):
	return backend.sqrt(backend.mean(backend.square(y_pred - y_true), axis=-1))

# COMMAND ----------

model.compile(optimizer="adam", loss="mse", metrics=["mse", rmse])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validation Data
# MAGIC 
# MAGIC Let's take a look at the [.fit()](https://keras.io/models/sequential/) method in the docs to see all of the options we have available! 
# MAGIC 
# MAGIC We can either explicitly specify a validation dataset, or we can specify a fraction of our training data to be used as our validation dataset.
# MAGIC 
# MAGIC The reason why we need a validation dataeset is to evaluate how well we are performing on unseen data (neural networks will overfit if you train them for too long!).
# MAGIC 
# MAGIC We can specify `validation_split` to be any value between 0.0 and 1.0 (defaults to 0.0).

# COMMAND ----------

history = model.fit(X_train, y_train, validation_split=.2, epochs=10, verbose=2)

# COMMAND ----------

# MAGIC %md
# MAGIC Wow! Look at how much lower our loss is to start, and that it is able to converge more quickly thanks to the data standardization!!
# MAGIC 
# MAGIC But, let's test: Is that RMSE correct?

# COMMAND ----------

import numpy as np

np.sqrt(history.history['mean_squared_error'][-1]) # Get MSE of last training epoch

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gotcha!! 
# MAGIC 
# MAGIC Because Keras computes the loss batch by batch, if we take the square root of the total MSE, it does not yield the same result as this RMSE function.
# MAGIC 
# MAGIC You can see François Chollet's [comment](https://github.com/keras-team/keras/issues/1170) on this issue, recommending to stick with MSE. But for teaching purposes, now you see how to wrtie custom metric functions!

# COMMAND ----------

model.compile(optimizer="adam", loss="mse", metrics=["mse"]) # Remove the RMSE metric

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Checkpointing
# MAGIC 
# MAGIC After each epoch, we want to save the model. However, we will pass in the flag `save_best_only=True`, which will only save the model if the validation loss decreased. This way, if our machine crashes or we start to overfit, we can always go back to the "good" state of the model.
# MAGIC 
# MAGIC To accomplish this, we will use the ModelCheckpoint [callback](https://keras.io/callbacks/). History is an example of a callback that is automatically applied to every Keras model.

# COMMAND ----------

from keras.callbacks import ModelCheckpoint

filepath = "/dbfs/user/" + username + '/keras_checkpoint_weights.hdf5'
checkpointer = ModelCheckpoint(filepath=filepath, verbose=1, save_best_only=True)

history = model.fit(X_train, y_train, validation_split=.2, epochs=10, verbose=2, callbacks=[checkpointer])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save Model/Load Model
# MAGIC 
# MAGIC Whenever you train neural networks, you want to save them. This way, you can reuse them later! With the checkpointing agove, we were saving the model weights. Let's try to load them into a new model.

# COMMAND ----------

try:
  newModel = Sequential()
  newModel.load_weights(filepath)
  
except ValueError as e:
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC We just saved our model weights with the checkpointing above. However, we also need the model configuration if we want to load the weights into a new model object.

# COMMAND ----------

from keras.models import model_from_yaml

yaml_string = model.to_yaml() # Returns a representation of the model as a YAML string (only model architecture, not weights)
newModel = model_from_yaml(yaml_string)

newModel.load_weights(filepath)

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, you can load in both the model and weights this way:

# COMMAND ----------

from keras.models import load_model

newModel = load_model(filepath)

# COMMAND ----------

# MAGIC %md
# MAGIC Check that the model architecture is the same.

# COMMAND ----------

newModel.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's train it for one more epoch (we need to recompile), and then save those weights.

# COMMAND ----------

newModel.compile(optimizer="adam", loss="mse")
newModel.fit(X_train, y_train, validation_split=.2, epochs=1, verbose=2)
newModel.save_weights(filepath)

# COMMAND ----------

# MAGIC %md
# MAGIC Now it's your turn to try out these techniques on the Boston Housing Dataset!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>