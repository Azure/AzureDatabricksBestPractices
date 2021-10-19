// Databricks notebook source
// MAGIC %md #Wind Turbine Predictive Maintenance using Streaming K-Means
// MAGIC 
// MAGIC In this example, we demonstrate anomaly detection for the purposes of finding damaged wind turbines. A damaged, a single inactive wind turbine costs energy utility companies thousands of dollars per day in losses.
// MAGIC 
// MAGIC Our dataset consists of vibration readings coming off sensors located in the gearboxes of wind turbines. We will use K-Means to measure the average normal vibration of a working, healthy wind turbine and then use the Within Sum of Squared Error to find wind turbines with abnormal vibrations which could be indicative of a failure.
// MAGIC 
// MAGIC *See image below for locations of the sensors*
// MAGIC 
// MAGIC ![WindTurbine](https://s3-us-west-2.amazonaws.com/databricks-demo-images/wind_turbine/wind_small.png)

// COMMAND ----------

// MAGIC %md ![Location](https://s3-us-west-2.amazonaws.com/databricks-demo-images/wind_turbine/wtsmall.png)

// COMMAND ----------

// MAGIC %md ##Steps for Analysis
// MAGIC 
// MAGIC 1. Import the Data
// MAGIC 2. Explore the Data
// MAGIC 3. Find the Optimal K
// MAGIC 4. Create a K-Means Model and Measure the Normal WSSSE
// MAGIC 5. Compare the Normal WSSSE to Damaged WSSSE
// MAGIC 6. Train and score vibration patterns coming from the Turbines in real-time

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._

// COMMAND ----------

// MAGIC %md ##1. Import the Data

// COMMAND ----------

// MAGIC %fs ls /windturbines/csv

// COMMAND ----------

val damagedSensorReadings = sqlContext.read.format("com.databricks.spark.csv").schema(
 StructType(StructField("AN10", DoubleType, false) :: StructField("AN3", DoubleType, false) :: StructField("AN4", DoubleType, false) :: StructField("AN5", DoubleType, false) :: StructField("AN6", DoubleType, false) :: StructField("AN7", DoubleType, false) :: StructField("AN8", DoubleType, false) :: StructField("AN9", DoubleType, false) :: StructField("SPEED", DoubleType, false) :: StructField("TORQUE", DoubleType, false) :: Nil)
).load("/windturbines/csv/D1*").drop("TORQUE")

// COMMAND ----------

val healthySensorReadings = sqlContext.read.format("com.databricks.spark.csv").schema(
 StructType(StructField("AN10", DoubleType, false) :: StructField("AN3", DoubleType, false) :: StructField("AN4", DoubleType, false) :: StructField("AN5", DoubleType, false) :: StructField("AN6", DoubleType, false) :: StructField("AN7", DoubleType, false) :: StructField("AN8", DoubleType, false) :: StructField("AN9", DoubleType, false) :: StructField("SPEED", DoubleType, false) :: Nil)
).load("/windturbines/csv/H1*")

// COMMAND ----------

healthySensorReadings.write.mode(SaveMode.Overwrite).saveAsTable("turbine_healthy")

// COMMAND ----------

damagedSensorReadings.write.mode(SaveMode.Overwrite).saveAsTable("turbine_damaged")

// COMMAND ----------

val turbine_damaged = table("turbine_damaged")
val turbine_healthy = table("turbine_healthy")

// COMMAND ----------

// MAGIC %md ##2. Explore the Data

// COMMAND ----------

display(turbine_healthy.describe())

// COMMAND ----------

display(turbine_damaged.describe())

// COMMAND ----------

val randomSample = turbine_healthy.withColumn("ReadingType", lit("HEALTHY")).sample(false, 500/4800000.0)
            .union(turbine_damaged.withColumn("ReadingType", lit("DAMAGED")).sample(false, 500/4800000.0))

// COMMAND ----------

display(randomSample)

// COMMAND ----------

// MAGIC %md ##3. Find the Optimal K

// COMMAND ----------

import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.clustering._
import org.apache.spark.mllib.linalg.Vectors

val models : Array[org.apache.spark.mllib.clustering.KMeansModel]  = new Array[org.apache.spark.mllib.clustering.KMeansModel](10)

val vectorAssembler = new VectorAssembler().setInputCols(turbine_healthy.columns.filter(_.startsWith("AN"))).setOutputCol("features")
val mmScaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaled")

val pipeline = new Pipeline()
  .setStages(Array(vectorAssembler, mmScaler))

val prepModel = pipeline.fit(turbine_healthy)
val prepData = prepModel.transform(turbine_healthy).cache()

val maxIter = 20
val maxK = 5
val findBestK = for (k <- 2 to maxK) yield {
  val kmeans = new KMeans().setK(k).setSeed(1L).setMaxIter(maxIter).setFeaturesCol("scaled")
  val model = kmeans.fit(prepData)
  val wssse = model.computeCost(prepData)
  (k, wssse)
}

// COMMAND ----------

val kWssseDf = sc.parallelize(findBestK).toDF("K", "wssse")
kWssseDf.registerTempTable("kWssseDf")
display(kWssseDf)

// COMMAND ----------

val previousDf = kWssseDf.withColumn("k", $"k"-1).withColumnRenamed("wssse", "previousWssse")
val derivativeOfWssse = previousDf.join(kWssseDf, "k").selectExpr("k", "previousWssse - wssse derivative").orderBy($"k")
display(derivativeOfWssse)


// COMMAND ----------

val bestK =  derivativeOfWssse
  .select(
    (lead("derivative", 1).over(Window.orderBy("k")) - $"derivative").as("nextDerivative")
  ,$"k").orderBy($"nextDerivative".desc).rdd.map(_(1)).first.asInstanceOf[Int]

// COMMAND ----------

// MAGIC %md ##4. Create a K-Means Model and Measure the Normal WSSSE

// COMMAND ----------

val kmeans = new KMeans().setK(bestK).setSeed(1L).setMaxIter(100).setFeaturesCol("scaled")
val bestModel = kmeans.fit(prepData)
val wssse = bestModel.computeCost(prepData)

// COMMAND ----------

val centroidCount = bestModel.transform(prepData).select("prediction").rdd.map(x =>
  (x.getInt(0), 1)
).reduceByKey(_ + _).toDF("centroid", "count")
display(centroidCount.orderBy($"centroid"))

// COMMAND ----------

// MAGIC %md ##5. Compare the Normal WSSSE to Damaged WSSSE

// COMMAND ----------

val prepDamagedModel = pipeline.fit(turbine_damaged)
val prepDamagedData = prepModel.transform(turbine_damaged).cache()
val bestDamagedModel = kmeans.fit(prepDamagedData)
val wssse = bestDamagedModel.computeCost(prepDamagedData)

// COMMAND ----------

//stop here

//throw new IllegalArgumentException()

// COMMAND ----------

// MAGIC %md ##6. Deploy the model using Streaming K-Means

// COMMAND ----------

prepDamagedData.count()

// COMMAND ----------

prepData.count()

// COMMAND ----------

package com.databricks.spark.windturbines
case class WsseTimestamps(ts : Long, wssse : Double)


// COMMAND ----------

import java.nio.ByteBuffer
import java.net._
import java.io._
import concurrent._
import scala.io._
import sys.process._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.receiver.Receiver
import sqlContext._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.util._
import scala.collection.mutable.SynchronizedQueue
import com.databricks.spark.windturbines._

val queue = new SynchronizedQueue[RDD[Vector]]()

dbutils.fs.rm("/tmp/windturbine", true)


val batchIntervalSeconds = 10

var newContextCreated = false      // Flag to detect whether new context was created or not

val kMeansModel = new StreamingKMeans()
  .setDecayFactor(0.5)
  .setK(2)
  .setRandomCenters(8, 0.1)
  
// Function to create a new StreamingContext and set it up
def creatingFunc(): StreamingContext = {
    
  // Create a StreamingContext
  val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
  val batchInterval = Seconds(batchIntervalSeconds)
  ssc.remember(Seconds(300))
 
  val dstream = ssc.queueStream(queue)
  
  kMeansModel.trainOn(dstream)
  dstream.foreachRDD {
    rdd => 
      val wssse = kMeansModel.latestModel().computeCost(rdd)
      val timestamp = System.currentTimeMillis / 1000
      sc.parallelize(
          Seq(
            WsseTimestamps(timestamp, wssse)
          )
        ).toDF().write.mode(SaveMode.Append).json("/tmp/windturbine")
  }
  
  
  
  /*
  dstream.foreachRDD { 
    rdd =>
      // if the RDD has data
       if(!(rdd.isEmpty())) {
           sqlContext.createDataFrame(rdd, rdd.take(1)(0).schema)
         // Append the results to our turbine_predictions
         .write.mode(SaveMode.Append).saveAsTable("turbine_predictions")
       } 
  }*/
  println("Creating function called to create new StreamingContext for Wind Turbine Predictions")
  newContextCreated = true  
  ssc
}

val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
if (newContextCreated) {
  println("New context created from currently defined creating function") 
} else {
  println("Existing context running or recovered from checkpoint, may not be running currently defined creating function")
}

ssc.start()

// COMMAND ----------

for(i <- 0 to 10) {
  val vectors = MLUtils.convertVectorColumnsFromML(prepData.select("scaled").sample(true, 0.0005)).rdd.map{row => row.getAs[Vector](0)}
  Thread.sleep(Seconds(3).milliseconds)
  queue += vectors
}
Thread.sleep(Seconds(3).milliseconds)
for(i <- 0 to 2) {
  val vectors = MLUtils.convertVectorColumnsFromML(prepDamagedData.select("scaled").sample(true, 0.0005)).rdd.map{row => row.getAs[Vector](0)}
  Thread.sleep(Seconds(3).milliseconds)
  queue += vectors
}

// COMMAND ----------

//ssc.stop()

// COMMAND ----------

val windTurbineResults = sqlContext.read.json("/tmp/windturbine")

// COMMAND ----------

display(windTurbineResults.orderBy("ts"))

// COMMAND ----------

