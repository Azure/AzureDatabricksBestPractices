# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns

# COMMAND ----------

def plot_confusion_matrix(confusion_matrix_uri):
  confmat = pd.read_pickle(confusion_matrix_uri)
  confmat = pd.pivot_table(confmat, values="count", index=["predicted_quality"], columns=["quality"], aggfunc=np.sum, fill_value=0)

  fig = plt.figure(figsize=(4,4))

  sns.heatmap(confmat, annot=True, fmt="d", square=True, cmap="OrRd")
  plt.yticks(rotation=0)
  plt.xticks(rotation=90)

  # display(fig)
  return fig

# COMMAND ----------

def plot_model_quality(df):
  sns.set(style='dark')
  sns.set()
  fig, ax = plt.subplots(figsize=(14,4))

  hue_order=['Accurate', 'Inaccurate']
  sns.lineplot(x='window_day', y='ratio', hue='accurate_prediction', hue_order=hue_order, style='accurate_prediction', style_order=hue_order, alpha=1, data = df.toPandas())
  plt.yticks(rotation=0)
  plt.xticks(rotation=0)
  plt.ylabel('% in population')
  plt.xlabel('Date')
  plt.title('Model Monitoring KPI over time')

  ax.axvline(x='2019-07-10', linewidth=1, linestyle='--', alpha=0.3)
  ax.axvline(x='2019-07-19', linewidth=1, linestyle='--', alpha=0.3)
  ax.axvline(x='2019-08-04', linewidth=1, linestyle='--', alpha=0.3)
  ax.axvline(x='2019-08-12', linewidth=1, linestyle='--', alpha=0.0)

  ax.legend(bbox_to_anchor=(1.1, 1.05))

  rect = patches.Rectangle(
      xy=(ax.get_xlim()[0], 80),  
      width=ax.get_xlim()[1]-ax.get_xlim()[0],  
      height=20,
      color='green', alpha=0.1, ec='red'
  )
  ax.add_patch(rect)

  fig.tight_layout()
  display(fig)
  plt.close(fig)

  return True

# COMMAND ----------

def plot_summary(df1, df2):
  sns.set(style='dark')
  sns.set()
  fig, ax = plt.subplots(figsize=(14,4))

  hue_order=['Accurate', 'Inaccurate']
  sns.lineplot(x='window_day', y='ratio', hue='accurate_prediction', hue_order=hue_order, style='accurate_prediction', style_order=hue_order, alpha=0.1, data = df1.toPandas())
  sns.lineplot(x='window_day', y='ratio', hue='accurate_prediction', hue_order=hue_order, style='accurate_prediction', style_order=hue_order, legend=False, data = df1.filter(df1.window_day < '2019-07-21').toPandas())
  sns.lineplot(x='window_day', y='ratio', hue='accurate_prediction', hue_order=hue_order, style='accurate_prediction', style_order=hue_order,legend=False, alpha=1, data = df2.filter(df2.window_day >= '2019-07-21').toPandas())
  plt.yticks(rotation=0)
  plt.xticks(rotation=0)
  plt.ylabel('% in population')
  plt.xlabel('Date')
  plt.title('Model Monitoring KPI over time')

  ax.axvline(x='2019-07-10', linewidth=1, linestyle='--', alpha=0.3)
  ax.axvline(x='2019-07-19', linewidth=1, linestyle='--', alpha=0.3)
  ax.axvline(x='2019-08-04', linewidth=1, linestyle='--', alpha=0.3)
  ax.axvline(x='2019-08-12', linewidth=1, linestyle='--', alpha=0.0)

  ax.legend(bbox_to_anchor=(1.1, 1.05))

  rect = patches.Rectangle(
      xy=(ax.get_xlim()[0], 80),  
      width=ax.get_xlim()[1]-ax.get_xlim()[0],  
      height=20,
      color='green', alpha=0.1, ec='red'
  )
  ax.add_patch(rect)

  fig.tight_layout()
  display(fig)
  plt.close(fig)

  return True