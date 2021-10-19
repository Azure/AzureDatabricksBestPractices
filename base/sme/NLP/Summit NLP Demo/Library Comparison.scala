// Databricks notebook source
// MAGIC %run "./Define UDFs"

// COMMAND ----------

// DBTITLE 1,Get InputText
// MAGIC %python
// MAGIC import pandas as pd
// MAGIC 
// MAGIC dbutils.widgets.text("inputText", "")
// MAGIC inputText = dbutils.widgets.get("inputText")
// MAGIC df = spark.createDataFrame(pd.DataFrame([inputText])).toDF("text")
// MAGIC   
// MAGIC df.createOrReplaceTempView("df")

// COMMAND ----------

// DBTITLE 1,Sentiment Prediction
val df = spark.table("df")

display(SentimentPipeline()
          .pretrained()
          .transform(df)
          .select($"*", $"sentiment".alias("sparknlp"))
          .drop("document", "token", "normal", "sentiment")
          .selectExpr("*", "sentimentTextBlobUDF(text) as textblob", "sentimentNLTKUDF(text) as nltk")
          .select($"text", $"textblob", $"nltk", $"sparknlp", sentiment($"text").alias("corenlp")))

// COMMAND ----------

