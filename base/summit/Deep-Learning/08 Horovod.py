# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Horovod
# MAGIC HorovodRunner is a general API to run distributed DL workloads on Databricks using Uber’s [Horovod](https://github.com/uber/horovod) framework. By integrating Horovod with Spark’s barrier mode, Databricks is able to provide higher stability for long-running deep learning training jobs on Spark. HorovodRunner takes a Python method that contains DL training code with Horovod hooks. This method gets pickled on the driver and sent to Spark workers. A Horovod MPI job is embedded as a Spark job using barrier execution mode. The first executor collects the IP addresses of all task executors using BarrierTaskContext and triggers a Horovod job using mpirun. Each Python MPI process loads the pickled program back, deserializes it, and runs it.
# MAGIC 
# MAGIC ![](http://databricks-docs-dev.s3-website-us-west-2.amazonaws.com/_images/horovod-runner.png)
# MAGIC 
# MAGIC *Requirements*: HorovodRunner requires Databricks Runtime 5.2+ ML (Beta).
# MAGIC 
# MAGIC * [Horovod Runner demo](https://vimeo.com/316872704/e79235f62c)
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Use Horovod to train a distributed neural network

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

from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# COMMAND ----------

import horovod.keras as hvd
import tensorflow as tf
tf.set_random_seed(42) # For reproducibility

from keras.models import Sequential
from keras.layers import Dense

def build_model():
  return Sequential([Dense(20, input_dim=8, activation='relu'),
                    Dense(20, activation='relu'),
                    Dense(1, activation='linear')]) # Keep the last layer as linear because this is a regression problem

# COMMAND ----------

# MAGIC %md
# MAGIC ## Broadcast Data

# COMMAND ----------

# MAGIC %md
# MAGIC IF using GPU:
# MAGIC 
# MAGIC * Horovod: pin GPU to be used to process local rank (one GPU per process)
# MAGIC * config = tf.ConfigProto()
# MAGIC * config.gpu_options.allow_growth = True
# MAGIC * config.gpu_options.visible_device_list = str(hvd.local_rank())
# MAGIC * sess = tf.Session(config=config)
# MAGIC * K.set_session(sess)

# COMMAND ----------

from keras import optimizers
from keras.callbacks import *
import shutil

def run_training_horovod():
  # Horovod: initialize Horovod.
  hvd.init()
  
  model = build_model()
  
  # Horovod: adjust learning rate based on number of GPUs.
  optimizer=optimizers.Adam(lr=0.001*hvd.size())
  
  # Horovod: add Horovod Distributed Optimizer.
  optimizer = hvd.DistributedOptimizer(optimizer)

  model.compile(optimizer=optimizer, loss="mse", metrics=["mse"])
  
  history = model.fit(X_train, y_train, validation_split=.2, epochs=20, batch_size=64, verbose=2)

# COMMAND ----------

from sparkdl import HorovodRunner
hr = HorovodRunner(np=-1)
hr.run(run_training_horovod)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Better Horovod

# COMMAND ----------

from keras import optimizers
from keras.callbacks import *
import shutil

def run_training_horovod():
  # Horovod: initialize Horovod.
  hvd.init()
  
  model = build_model()
  
  # Horovod: adjust learning rate based on number of GPUs.
  optimizer=optimizers.Adam(lr=0.001*hvd.size())
  
  # Horovod: add Horovod Distributed Optimizer.
  optimizer = hvd.DistributedOptimizer(optimizer)

  model.compile(optimizer=optimizer, loss="mse", metrics=["mse"])

  checkpoint_dir =     "/dbfs/user/" + username + "/horovod_checkpoint_weights.hdf5"
  tmp_checkpoint_dir = "/dbfs/user/" + username + "/temp/horovod_checkpoint_weights.hdf5"
  
  callbacks = [
    # Horovod: broadcast initial variable states from rank 0 to all other processes.
    # This is necessary to ensure consistent initialization of all workers when
    # training is started with random weights or restored from a checkpoint.
    hvd.callbacks.BroadcastGlobalVariablesCallback(0),

    # Horovod: average metrics among workers at the end of every epoch.
    # Note: This callback must be in the list before the ReduceLROnPlateau,
    # TensorBoard or other metrics-based callbacks.
    hvd.callbacks.MetricAverageCallback(),

    # Horovod: using `lr = 1.0 * hvd.size()` from the very beginning leads to worse final
    # accuracy. Scale the learning rate `lr = 1.0` ---> `lr = 1.0 * hvd.size()` during
    # the first five epochs. See https://arxiv.org/abs/1706.02677 for details.
    hvd.callbacks.LearningRateWarmupCallback(warmup_epochs=5, verbose=1),
    
    # Reduce the learning rate if training plateaues.
    ReduceLROnPlateau(patience=10, verbose=1)
  ]
  
  # Horovod: save checkpoints only on worker 0 to prevent other workers from corrupting them.
  if hvd.rank() == 0:
    callbacks.append(ModelCheckpoint(tmp_checkpoint_dir, save_weights_only=True))
  
  history = model.fit(X_train, y_train, validation_split=.2, epochs=20, batch_size=64, verbose=2, callbacks = callbacks)
  
  if hvd.rank() == 0:
    shutil.copyfile(tmp_checkpoint_dir, checkpoint_dir)

# COMMAND ----------

from sparkdl import HorovodRunner
hr = HorovodRunner(np=-1)
hr.run(run_training_horovod)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run on all workers

# COMMAND ----------

from sparkdl import HorovodRunner
hr = HorovodRunner(np=0)
hr.run(run_training_horovod)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>