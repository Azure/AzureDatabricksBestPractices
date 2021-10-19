# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Deployment
# MAGIC 
# MAGIC After batch deployment, model inference using Spark Streaming represents the second most popular deployment option.  This lesson introduces Spark Streaming and how to perform inference on a stream of incoming data.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Introduce predictions on streaming data
# MAGIC  - Connect to a Spark Stream
# MAGIC  - Predict using an `sklearn` model on a stream of data
# MAGIC  - Stream predictions into an always up-to-date parquet file

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Inference on Streaming Data
# MAGIC 
# MAGIC Spark Streaming enables scalable and fault-tolerant operations that continuously perform inference on incoming data.  Streaming applications can also incorporate ETL and other Spark features to trigger actions in real time.  This lesson is meant as an introduction to streaming applications as they pertain to production machine learning jobs.  
# MAGIC 
# MAGIC Streaming poses a number of specific obstacles. These obstacles include:<br><br>
# MAGIC 
# MAGIC * *End-to-end reliability and correctness:* Applications must be resilient to failures of any element of the pipeline caused by network issues, traffic spikes, and/or hardware malfunctions
# MAGIC * *Handle complex transformations:* applications receive many data formats that often involve complex business logic
# MAGIC * *Late and out-of-order data:* network issues can result in data that arrives late and out of its intended order
# MAGIC * *Integrate with other systems:* Applications must integrate with the rest of a data infrastructure
# MAGIC 
# MAGIC Streaming data sources in Spark offer the same DataFrames API for interacting with your data.  The crucial difference is that in structured streaming, the DataFrame is unbounded.  In other words, data arrives in an input stream and new records are appended to the input DataFrame.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/structured-streamining-model.png" style="height: 400px; margin: 20px"/></div>
# MAGIC 
# MAGIC Spark is a good solution for batch inference.  Streaming is a great solution for incoming streams of data.  For low-latency inference, however, Spark may or may not be the best solution depending on the latency demands of your task.  Real time serving will be covered in the next lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Connecting to the Stream
# MAGIC 
# MAGIC As data technology matures, the industry has been converging on a set of technologies.  Apache Kafka and the Azure managed alternative Event Hubs has become the ingestion engine at the heart of many pipelines.  
# MAGIC 
# MAGIC This technology brokers messages between producers, such as an IoT device writing data, and consumers, such as a Spark cluster reading data to perform real time analytics. There can be a many-to-many relationship between producers and consumers and the broker itself is scalable and fault tolerant.
# MAGIC 
# MAGIC We'll simulate a stream from Kafka using the `maxFilesPerTrigger` option.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/>  There are a number of ways to stream data.  One other common design pattern is to stream from an Azure Blob Container where any new files that appear will be read by the stream.

# COMMAND ----------

# MAGIC %md
# MAGIC Import the dataset in Spark using a standard import.

# COMMAND ----------

airbnbDF = spark.read.parquet("/mnt/conor-work/airbnb/airbnb-cleaned-mlflow.parquet")

display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a schema for the data stream.

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType, StructType

schema = (StructType()
.add("host_total_listings_count", DoubleType())
.add("neighbourhood_cleansed", IntegerType())
.add("zipcode", IntegerType())
.add("latitude", DoubleType())
.add("longitude", DoubleType())
.add("property_type", IntegerType())
.add("room_type", IntegerType())
.add("accommodates", DoubleType())
.add("bathrooms", DoubleType())
.add("bedrooms", DoubleType())
.add("beds", DoubleType())
.add("bed_type", IntegerType())
.add("minimum_nights", DoubleType())
.add("number_of_reviews", DoubleType())
.add("review_scores_rating", DoubleType())
.add("review_scores_accuracy", DoubleType())
.add("review_scores_cleanliness", DoubleType())
.add("review_scores_checkin", DoubleType())
.add("review_scores_communication", DoubleType())
.add("review_scores_location", DoubleType())
.add("review_scores_value", DoubleType())
.add("price", DoubleType())
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check to make sure the schemas match.

# COMMAND ----------

schema == airbnbDF.schema

# COMMAND ----------

# MAGIC %md
# MAGIC Check the number of shuffle partitions.

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC Change this to 8.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a data stream using `readStream` and `maxFilesPerTrigger`.

# COMMAND ----------

streamingData = (spark
                 .readStream
                 .schema(schema)
                 .option("maxFilesPerTrigger", 1)
                 .parquet("/mnt/conor-work/airbnb/airbnb-cleaned-mlflow.parquet")
                 .drop("price"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply an `sklearn` Model on the Stream
# MAGIC 
# MAGIC Using the DataFrame API, Spark allows us to interact with a stream of incoming data in much the same way that we did with a batch of data.  

# COMMAND ----------

# MAGIC %md
# MAGIC Import a `spark_udf`
# MAGIC 
# MAGIC TODO: make this generalizable by removing the `run_id`

# COMMAND ----------

import mlflow.pyfunc
pyfunc_udf = mlflow.pyfunc.spark_udf(spark, "random-forest-model", run_id="d90a61be93a547fda63cce8704114fb7")

# COMMAND ----------

# MAGIC %md
# MAGIC Transform the stream with a prediction.

# COMMAND ----------

predictionsDF = streamingData.withColumn("prediction", pyfunc_udf(*streamingData.columns))

display(predictionsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write out a Stream of Predictions
# MAGIC 
# MAGIC You can perform writes to any target database.  In this case, write to a parquet file.  This file will always be up to date, another component of an application can query this endpoint at any time.

# COMMAND ----------

checkpointLocation = userhome + "/academy/stream.checkpoint"
writePath = userhome + "/academy/predictions"

(predictionsDF
  .writeStream                                           # Write the stream
  .format("parquet")                                     # Use the delta format
  .partitionBy("zipcode")                                # Specify a feature to partition on
  .option("checkpointLocation", checkpointLocation)      # Specify where to log metadata
  .option("path", writePath)                             # Specify the output path
  .outputMode("append")                                  # Append new records to the output path
  .start()                                               # Start the operation
)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the underlying file.  Refresh this a few times.

# COMMAND ----------

spark.read.parquet(writePath).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Stop the stream.

# COMMAND ----------

[q.stop() for q in spark.streams.active]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** What are commonly approached as data streams?  
# MAGIC **Answer:** Apache Kafka and the Azure managed alternative Event Hubs are common data streams.  Additionally, it's common to monitor a directory for incoming files.  When a new file appears, it is brought into the stream for processing.
# MAGIC 
# MAGIC **Question:** How does Spark ensure exactly-once data delivery and maintain metadata on a stream?  
# MAGIC **Answer:** Checkpoints give Spark this fault tolerance through the ability to maintain state off of the cluster.
# MAGIC 
# MAGIC **Question:** How does the Spark approach to streaming integrate with other Spark features?  
# MAGIC **Answer:** Spark Streaming uses the same DataFrame API, allowing easy integration with other Spark functionality.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Real Time Deployment]($./08-Real-Time-Deployment ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on streaming ETL jobs?  
# MAGIC **A:** Check out the Databricks blog post <a href="https://databricks.com/blog/2017/01/19/real-time-streaming-etl-structured-streaming-apache-spark-2-1.html" target="_blank">Real-time Streaming ETL with Structured Streaming in Apache Spark 2.1</a>
# MAGIC 
# MAGIC **Q:** Where can I get more information on integrating Streaming and Kafka?  
# MAGIC **A:** Check out the <a href="https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html" target="_blank">Structured Streaming + Kafka Integration Guide</a>
# MAGIC 
# MAGIC **Q:** Where can I see a case study on an IoT pipeline using Spark Streaming?  
# MAGIC **A:** Check out the Databricks blog post <a href="https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html" target="_blank">Processing Data in Apache Kafka with Structured Streaming in Apache Spark 2.2</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>