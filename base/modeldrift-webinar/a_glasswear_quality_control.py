# Databricks notebook source
# MAGIC %md 
# MAGIC # Glassware Quality Control

# COMMAND ----------

# MAGIC %md
# MAGIC ### Objective: 
# MAGIC - To predict and grade the quality of the product as they are made, prior to actual inspection process which are a day or two later.
# MAGIC - This could act as an early indicator of quality for process optimization, and to send lower quality products for additional inspection and categorization.
# MAGIC 
# MAGIC ### About Data:
# MAGIC - sensor_reading - Sensor data from manufacturing equipment is streamed through IoT devices. Final aggregated data includes Temperature, and Pressure, along with process duration.
# MAGIC - product_quality - For each product id, actual quality categorization is available

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Architecture Reference
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/model_drift_architecture.png" width="1300">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### End to End Pipeline  
# MAGIC ### Data Access -> Data Prep -> Model Training -> Deployment -> Monitoring -> Action & Feedback Loop

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md ## <a>Data Access & Prep</a>

# COMMAND ----------

# MAGIC %md
# MAGIC __Setup access to blob storage, where data is streamed from and to, in delta lake__

# COMMAND ----------

# MAGIC %run ./data/data_access

# COMMAND ----------

# MAGIC %md
# MAGIC __Read sensor_reading & product_quality, join and prepare the data to be fed for model training__

# COMMAND ----------

# MAGIC %run ./data/data_prep

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assume today is 2019-07-11  
# MAGIC __Using data from (2019-07-01 - 2019-07-10) to generate models, for which sensor_reading and product_quality data are available__  
# MAGIC   
# MAGIC __To reproduce this demo, generate and push initial set of data using ./demo_utils/demo_data_init __

# COMMAND ----------

# MAGIC %md
# MAGIC __Prior to production workflow, typically one could setup a notebook for EDA, get familiar with dataset, and explore modeling methods__  
# MAGIC __Check EDA notebook example here, for this project__

# COMMAND ----------

today_date = '2019-07-11 18:00'

# COMMAND ----------

model_data_date = {'start_date':'2019-07-01 00:00:00', 'end_date':'2019-07-10 23:59:00'}
model_df = model_data(model_data_date)
display(model_df)

# COMMAND ----------

# MAGIC %md ## <a>Model Training & Tuning</a>

# COMMAND ----------

# MAGIC %md
# MAGIC __Run various models (Random Forest, Decision Tree, XGBoost), each with its own set of hyperparameters, and log all the information to MLflow__

# COMMAND ----------

# MAGIC %run ./quality_modeler/generate_models

# COMMAND ----------

# MAGIC %md ## <a>Model Selection & Deployment</a>

# COMMAND ----------

# MAGIC %md
# MAGIC __Search MLflow to find the best model from above experiment runs across all model types__

# COMMAND ----------

mlflow_search_query = "params.model_data_date = '"+ model_data_date['start_date']+ ' - ' + model_data_date['end_date']+"'"
best_run_details = best_run(mlflow_exp_id, mlflow_search_query)

print("Best run from all trials:" + best_run_details['runid'])
print("Params:")
print(best_run_details["params"])
print("Metrics:")
print(best_run_details["metrics"])

# COMMAND ----------

# MAGIC %md
# MAGIC __Mark the best run as production in MLflow, to be used during scoring__

# COMMAND ----------

push_model_production(mlflow_exp_id, best_run_details['runid'], userid, today_date)

# COMMAND ----------

# MAGIC %md ## <a>Model Scoring</a>

# COMMAND ----------

get_model_production(mlflow_exp_id)

# COMMAND ----------

# MAGIC %run ./quality_scorer/score_quality

# COMMAND ----------

# MAGIC %md
# MAGIC __Read the sensor_reading stream, apply model scoring, and write the output stream as 'predicted_quality' delta table__

# COMMAND ----------

sensor_reading_stream = stream_sensor_reading()
predict_stream = stream_score_quality(sensor_reading_stream)

# COMMAND ----------

# MAGIC %md ## <a>Model Monitoring & Feedback</a>

# COMMAND ----------

# MAGIC %run ./model_quality/model_quality_monitor

# COMMAND ----------

predicted_quality = get_predicted_quality()
product_quality = get_product_quality()
model_quality_summary = track_model_quality(product_quality, predicted_quality)

# COMMAND ----------

display(model_quality_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assume today is 2019-07-16

# COMMAND ----------

plot_model_quality(model_quality_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assume today is 2019-07-21
# MAGIC __To reproduce this demo, push data for range 2019-07-16 - 2019-07-21 using ./demo_utils/demo_data_init __

# COMMAND ----------

today_date = '2019-07-21 01:00'

# COMMAND ----------

model_quality_summary = track_model_quality(get_product_quality(), get_predicted_quality())
plot_model_quality(model_quality_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC We see drift occurs on 07/20 (based on process date)

# COMMAND ----------

# MAGIC %md ## <a>Retrain After Drift</a>

# COMMAND ----------

# MAGIC %md
# MAGIC __Read sensor_reading & product_quality, join and prepare the data to be fed for model training__

# COMMAND ----------

model_data_date = {'start_date':'2019-07-19 00:00:00', 'end_date':'2019-07-21 00:00:00'}
model_df = model_data(model_data_date)
display(model_df)

# COMMAND ----------

# MAGIC %md
# MAGIC __Run various models (Random Forest, Decision Tree, XGBoost), each with its own set of hyperparameters, and log all the information to MLflow__

# COMMAND ----------

# MAGIC %run ./quality_modeler/generate_models

# COMMAND ----------

# MAGIC %md
# MAGIC __Search MLflow to find the best model from above experiment runs across all model types__

# COMMAND ----------

mlflow_search_query = "params.model_data_date = '"+ model_data_date['start_date']+ ' - ' + model_data_date['end_date']+"'"
best_run_details = best_run(mlflow_exp_id, mlflow_search_query)

print("Best run from all trials:" + best_run_details['runid'])
print("Params:")
print(best_run_details["params"])
print("Metrics:")
print(best_run_details["metrics"])

# COMMAND ----------

# MAGIC %md
# MAGIC __Mark the best run as production in MLflow, to be used during scoring__

# COMMAND ----------

push_model_production(mlflow_exp_id, best_run_details['runid'], userid, today_date)

# COMMAND ----------

predict_stream.stop()
predict_stream = stream_score_quality(stream_sensor_reading())

# COMMAND ----------

# MAGIC %md
# MAGIC __Summary after retrain__

# COMMAND ----------

# MAGIC %md
# MAGIC __To reproduce this demo, push data for range > 2019-07-21 using ./demo_utils/demo_data_init __

# COMMAND ----------

model_quality_summary = track_model_quality(get_product_quality(), get_predicted_quality())
plot_model_quality(model_quality_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC __ Let's see what would have happened if model was not updated?__

# COMMAND ----------

predicted_quality = score_quality(get_sensor_reading(), 'acfdba1243dd458b9780d99ad25539ce')
model_quality_summary = track_model_quality(get_product_quality(), predicted_quality)
plot_model_quality(model_quality_summary)

# COMMAND ----------

# MAGIC %md ## <a>Summary</a>

# COMMAND ----------

predicted_quality = score_quality(get_sensor_reading(), 'acfdba1243dd458b9780d99ad25539ce')
model_quality_summary_1 = track_model_quality(get_product_quality(), predicted_quality)
predicted_quality = score_quality(get_sensor_reading(), '92846c89e8b44933b25d0e31af1c42d9')
model_quality_summary_2 = track_model_quality(get_product_quality(), predicted_quality)

# COMMAND ----------

plot_summary(model_quality_summary_1, model_quality_summary_2)