# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Advanced Keras Lab
# MAGIC 
# MAGIC Now we are going to take the following objectives we learned in the past lab, and apply them here! You will further improve upon your first model with the Boston housing dataset.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Perform data standardization
# MAGIC  - Generate a separate train/validation dataset
# MAGIC  - Create earlycheckpointing callback
# MAGIC  - Load and apply your saved model

# COMMAND ----------

# MAGIC %run "./Includes/Classroom Setup"

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
# MAGIC ## 1. Data Standardization
# MAGIC 
# MAGIC Go ahead and standardize our training and test features. 
# MAGIC 
# MAGIC Recap: Why do we want to standardize our features? Do we use the test statistics when computing the mean/standard deviation?

# COMMAND ----------

# ANSWER
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's use the same model architecture as in the previous lab.

# COMMAND ----------

import tensorflow as tf
tf.set_random_seed(42) # For reproducibility

from keras.models import Sequential
from keras.layers import Dense

model = Sequential()
model.add(Dense(50, input_dim=13, activation="relu"))
model.add(Dense(20, activation="relu"))
model.add(Dense(1))
model.summary()

model.compile(optimizer="adam", loss="mse", metrics=["mse"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validation Data
# MAGIC 
# MAGIC In the demo notebook, we showed how to use the validation_split method to split your data into training and validation dataset.
# MAGIC 
# MAGIC Keras also allows you to specify a dedicated validation dataset.
# MAGIC 
# MAGIC Split your training set into 75-25 train-validation split. 

# COMMAND ----------

# ANSWER
X_train_split, X_val, y_train_split, y_val = train_test_split(X_train,
                                                              y_train,
                                                              test_size=0.25,
                                                              random_state=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Callbacks
# MAGIC 
# MAGIC In the demo notebook, we covered how to implement the ModelCheckpointer callback (History is automically done for us).
# MAGIC 
# MAGIC Now, add the model checkpointing, and only save the best model. Also add a callback for EarlyStopping (if the model doesn't improve after 2 epochs, terminate training). You will need to set `patience=2` and `min_delta` to .0001.
# MAGIC 
# MAGIC Use the [callbacks documentation](https://keras.io/callbacks/) for reference!

# COMMAND ----------

# ANSWER 
from keras.callbacks import ModelCheckpoint, EarlyStopping

filepath = "/dbfs/user/" + username + '/keras_checkpoint_weights_lab.hdf5'
dbutils.fs.rm(filepath, recurse=True)
checkpointer = ModelCheckpoint(filepath=filepath, verbose=1, save_best_only=True)
earlyStopping = EarlyStopping(monitor='val_loss', min_delta=0.0001, patience=2, mode='auto')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fit Model
# MAGIC 
# MAGIC Now let's put everything together! Fit the model to the training and validation data with `epochs`=30, `batch_size`=32, and the 2 callbacks we defined above.
# MAGIC 
# MAGIC Take a look at the [.fit()](https://keras.io/models/sequential/) method in the docs for help.

# COMMAND ----------

# ANSWER
history = model.fit(X_train_split, y_train_split, validation_data=(X_val, y_val), epochs=30, batch_size=32, verbose=2, callbacks=[checkpointer, earlyStopping])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load Model
# MAGIC 
# MAGIC Load in the weights saved from this model via checkpointing to a new variable called `newModel`, and make predictions for our test data. Then compute the RMSE (and YES, I said RMSE!). See if you can do this without re-compiling the model!

# COMMAND ----------

# ANSWER
from keras.models import load_model

newModel = load_model(filepath)
y_pred = newModel.predict(X_test)

# COMMAND ----------

# ANSWER

from sklearn.metrics import mean_squared_error

rmse = np.sqrt(mean_squared_error(y_test, y_pred))
print(rmse)

# COMMAND ----------

# MAGIC %md
# MAGIC Nice job with learning Keras! We will now examine a different type of neural networks: Convolutional Neural Networks in the next lab.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>