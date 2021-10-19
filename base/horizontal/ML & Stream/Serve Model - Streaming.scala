// Databricks notebook source
// MAGIC %md
// MAGIC ![Credit Card ML](/files/img/stream_score.png)

// COMMAND ----------

// MAGIC %md #Stream and Score

// COMMAND ----------

// DBTITLE 1,Define Schema
import org.apache.spark.sql.types._

val streamSchema = new StructType()
  .add(StructField("rating",DoubleType,true))
  .add(StructField("review",StringType,true))
  .add(StructField("time",LongType,true))
  .add(StructField("title",StringType,true))
  .add(StructField("user",StringType,true))

// COMMAND ----------

// DBTITLE 1,Load Model
import org.apache.spark.ml.PipelineModel
val model = PipelineModel.load("/mnt/joewiden/amazon-model")

// COMMAND ----------

// DBTITLE 1,Stream Ingestion
spark.conf.set("spark.sql.shuffle.partitions", "4")

val inputStream = spark
  .readStream
  .schema(streamSchema)
  .option("maxFilesPerTrigger", 1)
  .json("/mnt/jason/amazon/amazon-stream-input")

// COMMAND ----------

import org.apache.spark.sql.functions._

display(spark.read.schema(streamSchema).json("/mnt/jason/amazon/amazon-stream-input").withColumn("source_file",input_file_name()))

// COMMAND ----------

// DBTITLE 1,Score New Data Against Model
val scoredStream = model.transform(inputStream)

// COMMAND ----------

// DBTITLE 1,Sink Scored Data
scoredStream.writeStream
  .format("memory")
  .queryName("stream")
  .start()

// COMMAND ----------

// MAGIC %md ##Query Stream

// COMMAND ----------

// MAGIC %sql select rating, label, prediction, probability, review from stream order by review

// COMMAND ----------

