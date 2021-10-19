# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC course_name="Deep-Learning"
# MAGIC 
# MAGIC None # Suppress output

# COMMAND ----------

# MAGIC %run "./Dataset-Mounts"

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Make sure the directory exists first.
# MAGIC dbutils.fs.mkdirs("dbfs:/user/" + username)
# MAGIC dbutils.fs.mkdirs("dbfs:/user/" + username + "/temp")
# MAGIC 
# MAGIC def waitForMLflow():
# MAGIC   try:
# MAGIC     import mlflow; print("""The module "mlflow" is attached and ready to go.""");
# MAGIC   except ModuleNotFoundError:
# MAGIC     print("""The module "mlflow" is not yet attached to the cluster, waiting...""");
# MAGIC     while True:
# MAGIC       try: import mlflow; print("""The module "mlflow" is attached and ready to go."""); break;
# MAGIC       except ModuleNotFoundError: import time; time.sleep(1); print(".", end="");
# MAGIC 
# MAGIC def waitForShap():
# MAGIC   try:
# MAGIC     import shap; print("""The module "shap" is attached and ready to go.""");
# MAGIC   except ModuleNotFoundError:
# MAGIC     print("""The module "shap" is not yet attached to the cluster, waiting...""");
# MAGIC     while True:
# MAGIC       try: import shap; print("""The module "shap" is attached and ready to go."""); break;
# MAGIC       except ModuleNotFoundError: import time; time.sleep(1); print(".", end="");
# MAGIC         
# MAGIC def waitForLime():
# MAGIC   try:
# MAGIC     import lime; print("""The module "lime" is attached and ready to go.""");
# MAGIC   except ModuleNotFoundError:
# MAGIC     print("""The module "lime" is not yet attached to the cluster, waiting...""");
# MAGIC     while True:
# MAGIC       try: import lime; print("""The module "lime" is attached and ready to go."""); break;
# MAGIC       except ModuleNotFoundError: import time; time.sleep(1); print(".", end="");
# MAGIC 
# MAGIC None # Suppress output

# COMMAND ----------

# MAGIC %scala
# MAGIC displayHTML("All done!")