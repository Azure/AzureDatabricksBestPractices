# Databricks notebook source
def model_data(model_data_date):
  dates = (model_data_date['start_date'], model_data_date['end_date'])
  sensor_reading = get_sensor_reading()
  sensor_reading = sensor_reading.filter(sensor_reading.process_time.between(*dates))
  product_quality = get_product_quality()
  product_quality = product_quality.filter(product_quality.qualitycheck_time.between(*dates))
  return sensor_reading.join(product_quality, "pid")