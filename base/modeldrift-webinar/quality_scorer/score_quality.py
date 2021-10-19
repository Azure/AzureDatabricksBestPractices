# Databricks notebook source
def score_quality(df, model_runid):
  
  run_details = get_run_details(model_runid)
  print('Using model version:'+ run_details['runid'])
  prod_model = mlflow.spark.load_model(run_details['spark-model'])
  predicted_quality = prod_model.transform(df)
  predicted_quality = predicted_quality.select('pid', 'process_time', 'predicted_quality')
  predicted_quality = predicted_quality.withColumn('model_version', F.lit(run_details['runid']))
  
  return predicted_quality

# COMMAND ----------

def stream_score_quality(df):
  
  prod_run_details = get_model_production(mlflow_exp_id)
  predicted_quality = score_quality(df, prod_run_details['runid'])
  
  predict_stream = (predicted_quality.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", predicted_quality_cp_blob)
  .start(predicted_quality_blob))
  
  return predict_stream