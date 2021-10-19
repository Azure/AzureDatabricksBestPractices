# Databricks notebook source
# MAGIC %sh whoami

# COMMAND ----------



# COMMAND ----------

import logging

from matplotlib import pyplot as plt
import pandas as pd
import os

import azureml.core
from azureml.core.experiment import Experiment
from azureml.core.workspace import Workspace
from azureml.core.dataset import Dataset
from azureml.train.automl import AutoMLConfig

# COMMAND ----------

print("You are currently using version", azureml.core.VERSION, "of the Azure ML SDK")

# COMMAND ----------

