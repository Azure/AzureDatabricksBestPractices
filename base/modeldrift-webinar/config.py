# Databricks notebook source
storage_account_name = "glasswaremfg"
storage_account_access_key = "keyxxxxxxxx=="

userid = 'add_uer_here'

# Data paths
sensor_reading_blob = "wasbs://aaa"
product_quality_blob = "wasbs://bbb"

predicted_quality_blob = "wasbs://ccc"
predicted_quality_cp_blob = "wasbs://ddd"


# Modeling & MLflow settings
mlflow_exp_loc = "/Users/<add_full_path_here>/ModelDrift_Webinar/model_registry/glassware_quality"
mlflow_exp_name = "Glassware Quality Predictor"
mlflow_exp_id = ""

model_compare_metric = 'accuracy'

# COMMAND ----------

# MAGIC %run ./utils/viz_utils

# COMMAND ----------

# MAGIC %run ./utils/mlflow_utils

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as F