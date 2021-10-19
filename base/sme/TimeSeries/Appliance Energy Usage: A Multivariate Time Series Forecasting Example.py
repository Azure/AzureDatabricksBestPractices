# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Appliance Energy Usage: A Multivariate Time Series Forecasting Example
# MAGIC Experimental data used to create regression models of appliances energy use in a low energy building.
# MAGIC 
# MAGIC 
# MAGIC #### Data Set Information:
# MAGIC 
# MAGIC The data set is at 10 min for about 4.5 months. The house temperature and humidity conditions were monitored with a ZigBee wireless sensor network. Each wireless node transmitted the temperature and humidity conditions around 3.3 min. Then, the wireless data was averaged for 10 minutes periods. The energy data was logged every 10 minutes with m-bus energy meters. Weather from the nearest airport weather station (Chievres Airport, Belgium) was downloaded from a public data set from Reliable Prognosis (rp5.ru), and merged together with the experimental data sets using the date and time column. Two random variables have been included in the data set for testing the regression models and to filter out non predictive attributes (parameters).
# MAGIC 
# MAGIC 
# MAGIC #### Original source of the dataset:
# MAGIC 
# MAGIC http://archive.ics.uci.edu/ml/datasets/Appliances+energy+prediction
# MAGIC 
# MAGIC 
# MAGIC #### Attribute Information:
# MAGIC For detailed list of all attributes and datatypes, please see the link above
# MAGIC 
# MAGIC 
# MAGIC Where indicated, hourly data (then interpolated) from the nearest airport weather station (Chievres Airport, Belgium) was downloaded from a public data set from Reliable Prognosis, rp5.ru. Permission was obtained from Reliable Prognosis for the distribution of the 4.5 months of weather data.

# COMMAND ----------

# MAGIC %md ### 1. Load the data

# COMMAND ----------

df = spark.read.csv('/mnt/ved-demo/timeseries/energydata_complete.csv', header=True)
display(df)

# COMMAND ----------

# MAGIC %md ### 2. Normalize the dataset

# COMMAND ----------

# For this problem, we randomly select feature columns except 'Appliances' which we are trying to predict

from pyspark.sql.functions import round

df = df.dropna() 
dataset = df.select('Press_mm_hg', 'T_out', 'RH_out', 'Windspeed', 'Tdewpoint', 'Appliances')

# Round up the columns data to one decimal point

columns = dataset.columns
for i in columns:
  dataset = dataset.withColumn(i, round(i, 1))
  
display(dataset)

# COMMAND ----------

from sklearn.preprocessing import StandardScaler
from keras.preprocessing.sequence import TimeseriesGenerator

dataset = dataset.collect() # should return a list 

# This estimator scales and translates each feature individually such that it is in the given range on the training set, e.g. between zero and one.
scaler = StandardScaler()
scaled = scaler.fit_transform(dataset)

# Create features and label dataset 
X = scaled[:] # All of the above columns
y = scaled[:, -1] # Appliances or the target/label column

# COMMAND ----------

# MAGIC %md ### 3. Train-Test Split and create Lagged Sequences

# COMMAND ----------

from sklearn.model_selection import train_test_split

# split into train and test sets
trainX, testX, trainY, testY = train_test_split(X, y, test_size=0.20, random_state=42, shuffle = False)

# # Create overlapping windows for training and testing datasets
timesteps = 144
train_generator = TimeseriesGenerator(trainX, trainY, length=timesteps, sampling_rate=1, batch_size=len(trainX)-timesteps)
test_generator = TimeseriesGenerator(testX, testY, length=timesteps, sampling_rate=1, batch_size=len(testX)-timesteps)

# COMMAND ----------

# MAGIC %md ### 4. Data Validation
# MAGIC 
# MAGIC ***Total number of samples in our data (n) = 19735***
# MAGIC 
# MAGIC **Before Time series Generator:** 
# MAGIC 
# MAGIC 1. Number of samples in training set (.8 * n) = 15788
# MAGIC 2. Number of samples in testing set (.2 * n) = 3947
# MAGIC 
# MAGIC **After Time series Generator:**
# MAGIC 
# MAGIC 1. Number of samples in the training feature set = training samples - length = 15644
# MAGIC 2. Number of samples in the testing label set = testing samples - length = 3803
# MAGIC 3. The shape of the input set should be (batch_size, timesteps, input_dim) [https://keras.io/layers/recurrent/]. 
# MAGIC 
# MAGIC In Step 1 & 2, we chose 5 features 'Press_mm_hg', 'T_out', 'RH_out', 'Windspeed', 'Tdewpoint', therefore the input dimension will be 5.

# COMMAND ----------

train_X, train_y = train_generator[0]
test_X, test_y = test_generator[0]

print("Total Records (n): {}".format(df.count()))
print("Number of samples in training set (.8 * n): trainX = {}".format(trainX.shape[0]))
print("Number of samples in testing set (.2 * n): testX = {}".format(testX.shape[0]))
print("Number of timesteps: {}".format(test_X.shape[1]))
print("Number of samples in training feature set (trainX - timesteps): {}".format(train_X.shape[0]))
print("Number of samples in testing feature set (testX - timesteps):: {}".format(test_X.shape[0]))

# COMMAND ----------

# MAGIC %md ### Start Tensorboard (Optional)

# COMMAND ----------

from time import time
from keras.callbacks import TensorBoard
tb_dir = '/tmp/tensorflow_log_dir/{}'.format(time())
tensorboard = TensorBoard(log_dir = tb_dir)
dbutils.tensorboard.start(tb_dir)

# COMMAND ----------

# MAGIC %md ### 5. Model Training

# COMMAND ----------

from keras.models import Sequential
from keras.layers import Dense, CuDNNLSTM, Dropout
from keras.layers.advanced_activations import LeakyReLU
from keras.callbacks import EarlyStopping
import mlflow
import mlflow.keras


# LSTM expects the input data in a specific 3D format of test sample size, time steps, no. of input features. We had defined the time steps as n_lag variable in previous step.  Time steps are the past observations that the network will learn from (e.g. backpropagation through time).

# For details on what individual hyperparameters mean, see here: https://github.com/keras-team/keras/blob/master/keras/layers/recurrent.py#L2051

units = 2
num_epoch = 1000
batch_size = 144

with mlflow.start_run(experiment_id=3133492):

  model = Sequential()
  model.add(CuDNNLSTM(units, input_shape=(train_X.shape[1], train_X.shape[2])))
  model.add(LeakyReLU(alpha=0.5)) 
  model.add(Dropout(0.1))
  model.add(Dense(1))


  # Stop training when a monitored quantity has stopped improving.
  callback = [EarlyStopping(monitor="val_loss", min_delta = 0.00001, patience = 50, mode = 'auto', restore_best_weights=True), tensorboard] 

  # Using regression loss function 'Mean Standard Error' and validation metric 'Mean Absolute Error'
  model.compile(loss='mse', optimizer='adam', metrics=['mae'])

# fit network
  history = model.fit_generator(train_generator, \
                                epochs=num_epoch, \
                                steps_per_epoch=batch_size, \
                                validation_data=(test_X, test_y), \
                                callbacks = callback, \
                                verbose=2, \
                                shuffle=False, \
                                initial_epoch=0)

  mlflow.log_param("Neurons", units)
  mlflow.log_param("Batch Size", batch_size)
  mlflow.log_param("Epochs", num_epoch)
  mlflow.log_param("Notes", "")

#   Return loss value and metric value
  score = model.evaluate_generator(test_generator, verbose=0)   
  mlflow.log_metric("Test Loss", score[0]) 
  mlflow.log_metric("MAE", score[1])   
  mlflow.log_metric("Actual Epochs", len(history.history['loss']))
  mlflow.keras.log_model(model, "LSTM Model")
    
  # The model can be saved for future use and move to production
#   mlflow.keras.save_model(model1, "/dbfs/ved-demo/timeseries/best-appliance-model/")

# COMMAND ----------

# MAGIC %md ### 6. Model Evaluation
# MAGIC 
# MAGIC 1. Use Mlflow to track model runs and experiments over time 
# MAGIC 2. Use the plot to visualize the network
# MAGIC 3. Use graph below to check for fit

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/lstm_ved

# COMMAND ----------

# dbutils.fs.put("dbfs:/databricks/init/%s/graphviz.sh" % "vj-dl-py3","""
# #!/bin/bash
# sudo apt-get install graphviz --assume-yes
# """, True)

# COMMAND ----------

from keras.utils import plot_model
plot_model(model, to_file='/dbfs/FileStore/lstm_ved/model.png', show_shapes=True)
displayHTML("<img src ='/dbfs/FileStore/lstm_ved/model.png'/>")

# COMMAND ----------

# Plot train and test errors

from matplotlib import pyplot
import numpy as np

# Calculate the train loss and train metric, in this case mean absolute error
train_loss = np.mean(history.history['loss'])
train_mae = np.mean(history.history['mean_absolute_error'])

title = 'Train Loss: {0:.3f} Test Loss: {1:.3f}\n  Train MAE: {2:.3f}, Val MAE: {3:.3f}'.format(train_loss, score[0], train_mae, score[1])

# Plot loss function
fig = pyplot.figure()
pyplot.style.use('seaborn')

pyplot.plot(history.history['loss'], 'c-', label='train')
pyplot.plot(history.history['val_loss'], 'm:', label='test')
# pyplot.text(epoch-2, 0.07, rmse , style='italic')
pyplot.title(title)
pyplot.legend()
pyplot.grid(True)
fig.set_size_inches(w=7,h=7)
pyplot.close()
display(fig)

# COMMAND ----------

# Make predictions
yhat_train = model.predict_generator(train_generator)
yhat_test = model.predict_generator(test_generator)


# Shift predictions for plotting

# training results
yhat_train_plot = np.empty_like(y)
yhat_train_plot[:] = np.nan
yhat_train.shape = yhat_train.shape[0]
yhat_test.shape = yhat_test.shape[0]
yhat_train_plot.shape = yhat_train_plot.shape[0]
yhat_train_plot[timesteps:len(yhat_train)+timesteps] = yhat_train
  
#test results
yhat_test_plot = np.empty_like(y)
yhat_test_plot[:] = np.nan
yhat_train.shape = yhat_train.shape[0]
yhat_test.shape = yhat_test.shape[0]
yhat_test_plot.shape = yhat_test_plot.shape[0]
yhat_test_plot[len(yhat_train)+(timesteps*2):len(y)] = yhat_test
# len(targets), len(yhat_train), len(yhat_test_plot)

# COMMAND ----------

# Graph predictions 


from matplotlib import pyplot

fig = pyplot.figure()
pyplot.style.use('seaborn')
palette = pyplot.get_cmap('Set1')
pyplot.plot(y, marker='', color=palette(4), linewidth=1, alpha=0.9, linestyle=':', label='actual')
pyplot.plot(yhat_train_plot, marker='', color=palette(2), linewidth=1, alpha=0.9, label='training predictions')
pyplot.plot(yhat_test_plot, marker='', color=palette(3), linewidth=1, alpha=0.9, label='testing predictions')

pyplot.title('Appliances Energy Prediction', loc='center', fontsize=20, fontweight=5, color='orange')
pyplot.ylabel('Energy used (Wh)')
pyplot.legend()
fig.set_size_inches(w=15,h=5)
pyplot.close()

display(fig)

# COMMAND ----------

# Analysis of predictions - Standard Deviation 

# Old results for Standard Deviation
# Actual readings: 0.096
# Training predictions: 0.011
# Testing predictions: 0.008
  
print('Standard Deviation')
print('Actual readings: {0:.3f}'.format(np.std(y)))
print('Training predictions: {0:.3f}'.format(np.std(yhat_train)))
print('Testing predictions: {0:.3f}'.format(np.std(yhat_test)))

# COMMAND ----------

fig = pyplot.figure()
bins = np.linspace(-1, 0, 1)
pyplot.hist([y, yhat_train, yhat_train])
# pyplot.hist(yhat_test)
# pyplot.hist(yhat_train)
display(fig)

# COMMAND ----------

