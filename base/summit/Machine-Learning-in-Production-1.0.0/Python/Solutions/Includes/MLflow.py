# Databricks notebook source
# MAGIC 
# MAGIC %scala
# MAGIC // Use this if needed to programatically install python PIP libraries across an MLRuntime cluster
# MAGIC // package installPackages
# MAGIC //   object ConfigDistributor {
# MAGIC //     lazy val distribute = {
# MAGIC //       import sys.process._
# MAGIC //       """pip install sklearn""" !!
# MAGIC //     }
# MAGIC //   }    
# MAGIC // 
# MAGIC // val df = spark.range(0, sc.defaultParallelism, 1, sc.defaultParallelism)
# MAGIC //                 .map(x => (x, installPackages.ConfigDistributor.distribute))
# MAGIC //                 .toDF("index", "output")
# MAGIC // df.count
# MAGIC // // display(df)

# COMMAND ----------

from databricks_cli.configure.provider import get_config
import os

os.environ["MLFLOW_AUTODETECT_EXPERIMENT_ID"] = 'true'
os.environ['DATABRICKS_HOST'] = get_config().host
os.environ['DATABRICKS_TOKEN'] = get_config().token

# Silence YAML deprecation issue https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load(input)-Deprecation
os.environ["PYTHONWARNINGS"] = 'ignore::yaml.YAMLLoadWarning' 

# Can use to install mlflow if using DBR, not ML Runtime
# dbutils.library.installPyPI("mlflow==0.9.0")
# dbutils.library.restartPython()

displayHTML("Initialized environment variables for MLflow server...")
