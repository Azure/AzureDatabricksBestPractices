# Databricks notebook source

from pyspark.sql.functions import * 
from pyspark.sql import DataFrame

def statFileWithPartitions(input_path_para, partsCol, input_type):

  from functools import reduce
  
  # input_path_para has to have / at the end
  if (input_path_para[-1] != "/"):
    input_path_para += "/"

  input_path = input_path_para
  
  oneMB = 1048576.0   #1MB

  if (input_type == "parquet"):
    rawDF = spark.read.parquet(input_path)
  else:
    rawDF = spark.read.csv(input_path)
    
  rawDFStat = rawDF.selectExpr(*partsCol).distinct()  # inject iterable values 
   
  listOfPartitions = rawDFStat.rdd.map(lambda a: ("year=" + str(a[0]) + "/month=" + str(a[1]), str(a[0]) + "_" + str(a[1]))).collect()
      
  def func(args):
    lv = args[1]
    partPath = args[0]
    fullPath = input_path + partPath
    filesDF = spark.createDataFrame(dbutils.fs.ls(fullPath))
    
    if input_type == 'parquet':
      tempDF = filesDF.where("name like 'part-%'")
    else:
      tempDF = filesDF
    
    statDF = (tempDF
      .agg(count("name").alias("num_file")
      ,(max("size")/oneMB).alias("max_size")
      ,(min("size")/oneMB).alias("min_size")
      ,(avg("size")/oneMB).alias("avg_size")
      ,(sum("size")/oneMB).alias("part_size"))
    ) 
    return statDF.withColumn("partition",lit(lv))
  
  statArr = map(func, listOfPartitions)
  statDF = reduce(lambda df1, df2: df1.union(df2), statArr)
  
  return statDF

def statFileFolder(input_path):  
  oneMB = 1048576.0 #1MB
  filesDF = dbutils.fs.ls(input_path).toDF
  statDF = (filesDF
    .where("name like 'part-%'")
    .agg(count("name").alias("num_file")
    ,(max("size")/oneMB).alias("max_size")
    ,(min("size")/oneMB).alias("min_size")
    ,(avg("size")/oneMB).alias("avg_size")
    ,(sum("size")/oneMB).alias("part_size"))
   )
            
  return statDF
