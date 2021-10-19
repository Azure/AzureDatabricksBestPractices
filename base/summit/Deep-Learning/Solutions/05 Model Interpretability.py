# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Model Interpretability
# MAGIC 
# MAGIC <img style="width:20%" src="https://files.training.databricks.com/images/Limes.jpg" > No, we're not talking about limes.
# MAGIC 
# MAGIC We're talking about [Local Interpretable Model-Agnostic Explanations](https://github.com/marcotcr/lime).
# MAGIC 
# MAGIC Required Libraries (PyPi):
# MAGIC * lime
# MAGIC * shap
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Use LIME and SHAP to understand which features are important with the end prediction

# COMMAND ----------

# MAGIC %run "./Includes/Classroom Setup"

# COMMAND ----------

# Wait for the shap & mlflow modules to attactch to our cluster
# Utility method defined in Classroom-Setup
waitForShap()
waitForLime()

# COMMAND ----------

# MAGIC %md
# MAGIC Don't forget you need to scale your data because the model was trained on scaled data!

# COMMAND ----------

from sklearn.datasets import load_boston
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import numpy as np
np.random.seed(0)

boston_housing = load_boston() 

# split 80/20 train-test
X_train, X_test, y_train, y_test = train_test_split(boston_housing.data,
                                                        boston_housing.target,
                                                        test_size=0.2,
                                                        random_state=1)

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

print(boston_housing.DESCR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load in Saved Model

# COMMAND ----------

from keras.models import load_model

filepath = "/dbfs/mnt/training/bostonhousing/model.hdf5"

model = load_model(filepath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using LIME for Model Explaination
# MAGIC We can use the [LIME](https://github.com/marcotcr/lime) library to provide explainations of individual predictions.
# MAGIC 
# MAGIC ![](https://raw.githubusercontent.com/marcotcr/lime/master/doc/images/lime.png)

# COMMAND ----------

import lime
from lime.lime_tabular import LimeTabularExplainer

help(LimeTabularExplainer)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Interpreting Results
# MAGIC LIME explains the impact of each feature on the prediction, and tabular explainers need a training set. 
# MAGIC 
# MAGIC The reason for this is because we compute statistics on each feature (column). If the feature is numerical, we compute the mean and std, and discretize it into quartiles.

# COMMAND ----------

def model_predict(input):
  '''
  Convert keras prediction output to LIME compatible form. (Needs to output a 1 dimensional array)
  '''
  return model.predict(input).flatten()

explainer = LimeTabularExplainer(X_train, feature_names=boston_housing.feature_names, class_names=['price'], mode='regression')
# NOTE: In order to pass in categorical_features, they all need to be ints

# COMMAND ----------

# MAGIC %md
# MAGIC From the LIME docs:
# MAGIC 
# MAGIC 0. First, we generate neighborhood data by randomly hiding features from the instance. 
# MAGIC 0. We then learn locally weighted linear models on this neighborhood data to explain each of the classes in an interpretable way.

# COMMAND ----------

exp = explainer.explain_instance(X_test[0], model_predict, num_features=10)
print(f"True value: {y_test[0]}")
print(f"Local predicted value: {exp.predicted_value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which features are most important?

# COMMAND ----------

displayHTML(exp.as_html())

# COMMAND ----------

# MAGIC %md
# MAGIC Positive correlation: 
# MAGIC 0. `RM` (average number of rooms per dwelling) 
# MAGIC 0. `CRIM` (per capita crime rate by town) - below "average"
# MAGIC 0. `LSTAT` (% lower status of the population)
# MAGIC 0. `B`     (1000(Bk - 0.63)^2 where Bk is the proportion of blacks by town)
# MAGIC 0. `TAX`   (full-value property-tax rate per $10,000)
# MAGIC 0. `INDUS` (proportion of non-retail business acres per town)
# MAGIC 
# MAGIC Negative correlation:
# MAGIC 0. `CHAS` (Charles River dummy variable = 1 if tract bounds river; 0 otherwise)
# MAGIC 0. `ZN` (proportion of residential land zoned for lots over 25,000 sq.ft.)
# MAGIC 0. `RAD` (index of accessibility to radial highways)
# MAGIC 0. `PTRATIO`  (pupil-teacher ratio by town)
# MAGIC 
# MAGIC Do these make sense?

# COMMAND ----------

display(exp.as_pyplot_figure().tight_layout())

# COMMAND ----------

# MAGIC %md
# MAGIC Let's get those values as a list.

# COMMAND ----------

exp.as_list()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SHAP
# MAGIC 
# MAGIC SHAP ([SHapley Additive exPlanations](https://github.com/slundberg/shap)) is another approach to explain the output of a machine learning model. See the [SHAP NIPS](http://papers.nips.cc/paper/7062-a-unified-approach-to-interpreting-model-predictions) paper for details.
# MAGIC 
# MAGIC ![](https://raw.githubusercontent.com/slundberg/shap/master/docs/artwork/shap_diagram.png)
# MAGIC 
# MAGIC Great [blog post](https://blog.dominodatalab.com/shap-lime-python-libraries-part-1-great-explainers-pros-cons/) comparing LIME to SHAP. SHAP provides greater theoretical guarantees than LIME, but at the cost of additional compute. 

# COMMAND ----------

import shap
help(shap.DeepExplainer)

# COMMAND ----------

from IPython.core.display import display, HTML
import matplotlib.pyplot as plt

shap.initjs()
shap_explainer = shap.DeepExplainer(model, X_train[:200])
shap_values = shap_explainer.shap_values(X_test[0:1])
y_pred = model.predict(X_test[0:1])
print(f'Actual price: {y_test[0]}, Predicted price: {y_pred[0][0]}')
                   
# Saving to File b/c can't display IFrame directly in Databricks: https://github.com/slundberg/shap/issues/101
shap.save_html("shap.html", shap.force_plot(shap_explainer.expected_value[0], shap_values[0],X_test[0:1],
                                            feature_names=boston_housing.feature_names,show=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize
# MAGIC 
# MAGIC * Red pixels increase the model's output while blue pixels decrease the output.

# COMMAND ----------

import codecs
f=codecs.open("shap.html", 'r')
displayHTML(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC The values on the bottom show the true values of `X_test[0]`.

# COMMAND ----------

import pandas as pd
pd.DataFrame(X_test[0], boston_housing.feature_names, ["features"])

# COMMAND ----------

# MAGIC %md
# MAGIC This says that, overall, factors like negative LSTAT had the most positive effect on predicted housing price, while ZN had the most negative, among others.
# MAGIC ```
# MAGIC - ZN       proportion of residential land zoned for lots over 25,000 sq.ft.
# MAGIC - AGE      proportion of owner-occupied units built prior to 1940
# MAGIC - PTRATIO  pupil-teacher ratio by town
# MAGIC - RAD      index of accessibility to radial highways
# MAGIC - CHAS     Charles River dummy variable (= 1 if tract bounds river; 0 otherwise)
# MAGIC 
# MAGIC - LSTAT    % lower status of the population
# MAGIC - RM       average number of rooms per dwelling
# MAGIC - INDUS    proportion of non-retail business acres per town
# MAGIC - TAX      full-value property-tax rate per $10,000
# MAGIC - B        1000(Bk - 0.63)^2 where Bk is the proportion of blacks by town
# MAGIC - CRIM     per capita crime rate by town
# MAGIC - NOX      nitric oxides concentration (parts per 10 million)
# MAGIC - DIS      weighted distances to five Boston employment centres
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see what the values corresponding to each feature are.

# COMMAND ----------

shap_features = pd.DataFrame(shap_values[0][0], boston_housing.feature_names, ["features"])
shap_features.sort_values("features")

# COMMAND ----------

# MAGIC %md
# MAGIC The predicted value is the sum of the SHAP features + the average.

# COMMAND ----------

print(f"The predicted value is {shap_features.sum()[0] + shap_explainer.expected_value[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Question**: How similar are the LIME predictions to the SHAP predictions?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>