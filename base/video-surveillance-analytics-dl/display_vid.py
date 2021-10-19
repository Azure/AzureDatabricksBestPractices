# Databricks notebook source
def displayVid(filepath):
  return displayHTML("""
  <video width="480" height="320" controls>
  <source src="/files/%s?o=984752964297111" type="video/mp4">
  </video>
  """ % filepath)

# COMMAND ----------



# COMMAND ----------

def displayImg(filepath):
  dbutils.fs.cp(filepath, "FileStore/%s" % filepath)
  return displayHTML("""
  <img src="/files/%s?o=984752964297111">
  """ % filepath)

# COMMAND ----------

