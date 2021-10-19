# Databricks notebook source
from pyspark.sql import Window
import pyspark.sql.functions as F

# COMMAND ----------

def track_model_quality(real, predicted):
  quality_compare = predicted.join(real, "pid")
  quality_compare = quality_compare.withColumn(
      'accurate_prediction',
      F.when((F.col('quality')==F.col('predicted_quality')), 1)\
      .otherwise(0)
  )
  
  accurate_prediction_summary = (quality_compare.groupBy(F.window(F.col('process_time'), '1 day').alias('window'), F.col('accurate_prediction'))
                      .count()
                      .withColumn('window_day', F.expr('to_date(window.start)'))
                      .withColumn('total',F.sum(F.col('count')).over(Window.partitionBy('window_day')))
                      .withColumn('ratio', F.col('count')*100/F.col('total'))
                      .select('window_day','accurate_prediction', 'count', 'total', 'ratio')
                      .withColumn('accurate_prediction', F.when(F.col('accurate_prediction')==1, 'Accurate').otherwise('Inaccurate'))
                      .orderBy('window_day')
                     )
  return accurate_prediction_summary