# Databricks notebook source
# MAGIC %run ../utils/data_generator

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="/files/tables/databricks_squarelogo-e0ec9.png" width="45px"> Data Sample</h2>

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="/files/tables/databricks_squarelogo-e0ec9.png" width="45px"> Data Summary</h2>

# COMMAND ----------

display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="/files/tables/databricks_squarelogo-e0ec9.png" width="45px"> Temperature Over Time</h2>

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="/files/tables/databricks_squarelogo-e0ec9.png" width="45px"> Pressure Over Time</h2>

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="/files/tables/databricks_squarelogo-e0ec9.png" width="45px"> Duration Over Time</h2>

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="/files/tables/databricks_squarelogo-e0ec9.png" width="45px"> Is there an easy explanation between these variables and quality? Are the variables related to each other?</h2>

# COMMAND ----------

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

features = ['temp', 'pressure', 'duration', 'quality']
sampled_data = df.select(features).sample(False, 0.99).toPandas()

axs = pd.scatter_matrix(sampled_data, alpha=0.2,  figsize=(7, 7))
n = len(sampled_data.columns)
for i in range(n):
    v = axs[i, 0]
    v.yaxis.label.set_rotation(0)
    v.yaxis.label.set_ha('right')
    v.yaxis.label.set_size(6)
    h = axs[n-1, i]
    h.xaxis.label.set_rotation(90)
    h.xaxis.label.set_size(6)
display(plt.show())

# COMMAND ----------

