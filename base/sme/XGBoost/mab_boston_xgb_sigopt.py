# Databricks notebook source
import os
import warnings
import sys

import xgboost as xgb
from sigopt import Connection
from sigopt_sklearn.search import SigOptSearchCV
from xgboost.sklearn import XGBRegressor
import pandas as pd
import numpy as np
import mlflow
from mlflow import sklearn
from sklearn import metrics
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn import model_selection

# COMMAND ----------

boston = datasets.load_boston()
bostonDF = pd.DataFrame(boston.data)
bostonDF.columns = boston.feature_names
bostonDF['PRICE'] = boston.target
bostonDF.describe()

# COMMAND ----------

X, y = bostonDF.iloc[:,:-1],bostonDF.iloc[:,-1]
X_train, X_test, y_train, y_test = train_test_split(X,y, test_size=0.2, random_state = 123)

# COMMAND ----------

xgb_baseline = XGBRegressor(seed=123, eval_metric="rmse")
xgb_baseline.fit(X_train, y_train)
preds_baseline = xgb_baseline.predict(X_test)
rmse = np.sqrt(metrics.mean_squared_error(y_test, preds_baseline))
print("Baseline RMSE: %f" % (rmse))

# COMMAND ----------

exp_name="boston_xgb_sigopt"
conn = Connection(client_token="JBHGSGWPSYGKJPTBOICLIYHCQNOTZSXBRGHCMUDBKCHJKPMK")
experiment = conn.experiments().create(
  name=exp_name,
  parameters=[
    dict(name="log_learning_rate", bounds=dict(min=-5,max=-0.5), type="double"),
    dict(name="max_depth", bounds=dict(min=2,max=10), type="int"),
    dict(name="n_estimators", bounds=dict(min=10,max=200), type="int"),
    dict(name="subsample", bounds=dict(min=0.5,max=0.9), type="double"),
    dict(name="colsample_bytree", bounds=dict(min=0.5,max=0.9), type="double"),
    dict(name="min_child_weight", bounds=dict(min=1,max=10), type="int"),
    dict(name="gamma", bounds=dict(min=1,max=20), type="double")
    ],
  metadata=dict(
    template="python_xgboost_gbm"
    ),
  observation_budget=70
  )

# COMMAND ----------

def create_model(assignments):
    xgb_model = XGBRegressor(
      seed=123,
      learning_rate=10**assignments['log_learning_rate'],
      max_depth=assignments['max_depth'],
      n_estimators=assignments['n_estimators'],
      subsample=assignments['subsample'],
      colsample_bytree=assignments['colsample_bytree'],
      min_child_weight=assignments['min_child_weight'],
      gamma=assignments['gamma'],
      eval_metric="rmse"
    )
    xgb_model.fit(X_train, y_train)

    return xgb_model

def evaluate_model(assignments):
    xgb_model = create_model(assignments)
    pred = xgb_model.predict(X_train)
    return (-1*np.sqrt(metrics.mean_squared_error(pred, y_train)))

# COMMAND ----------

for _ in range(experiment.observation_budget):
    suggestion = conn.experiments(experiment.id).suggestions().create()
    assignments = suggestion.assignments
    value = evaluate_model(assignments)

    conn.experiments(experiment.id).observations().create(
        suggestion=suggestion.id,
        value=value
    )

# COMMAND ----------

assignments = conn.experiments(experiment.id).best_assignments().fetch().data[0].assignments
print(assignments)

# COMMAND ----------

# This is a SigOpt-tuned model
xgb_fit = create_model(assignments)

# COMMAND ----------

preds_is_baseline = xgb_baseline.predict(X_train)
rmse_is_baseline = np.sqrt(metrics.mean_squared_error(y_train, preds_is_baseline))
print("Baseline in sample RMSE: %f" % (rmse_is_baseline))
preds_os_baseline = xgb_baseline.predict(X_test)
rmse_os_baseline = np.sqrt(metrics.mean_squared_error(y_test, preds_os_baseline))
print("Baseline out of sample RMSE: %f" % (rmse_os_baseline))
preds_is_fit = xgb_fit.predict(X_train)
rmse_is_fit = np.sqrt(metrics.mean_squared_error(y_train, preds_is_fit))
print("Fit in sample RMSE: %f" % (rmse_is_fit))
preds_os_fit = xgb_fit.predict(X_test)
rmse_os_fit = np.sqrt(metrics.mean_squared_error(y_test, preds_os_fit))
print("Fit out of sample RMSE: %f" % (rmse_os_fit))