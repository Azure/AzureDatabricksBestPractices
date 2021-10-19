# Databricks notebook source
# MAGIC %sh sudo apt-get install graphviz --assume-yes

# COMMAND ----------

from torchviz import make_dot
import graphviz

# COMMAND ----------

def visualize_model(data, loc):
  model_viz = make_dot(data)
  model_viz.format = 'svg'
  model_viz.save(loc)
  model_viz.render()