// Databricks notebook source
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, BasicAWSCredentials}
import java.nio.ByteBuffer
import scala.util.Random
import com.google.gson.Gson
import org.joda.time
import org.joda.time.format._
import java.sql.Timestamp;
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %run ./clickHelper

// COMMAND ----------

// === Configurations for Kinesis streams ===
val aws_kinesis_keys = get_kinesis_creds
val kinesisStreamName = get_kinesis_stream
val kinesisEndpointUrl = get_kinesis_endpoint() // e.g. https://kinesis.us-west-2.amazonaws.com"

// === Configurations of amount of data to produce ===
val recordsPerSecond = 10
val wordsPerRecord = 10
val numSecondsToSend = 6000000

// COMMAND ----------

// MAGIC %sql describe paul.clicks

// COMMAND ----------

// Create the low-level Kinesis Client from the AWS Java SDK.
val kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(aws_kinesis_keys(0), aws_kinesis_keys(1)))
kinesisClient.setEndpoint(kinesisEndpointUrl)

println(s"Putting records onto stream $kinesisStreamName and endpoint $kinesisEndpointUrl at a rate of" +
  s" $recordsPerSecond records per second with $wordsPerRecord words per record for $numSecondsToSend seconds")

// Function to generate data

val impressions = spark.sql("select * from paul.clicks").collect()

case class Record(uid: String, clickTimestamp: String, exchangeID: Integer, publisher: String, creativeID: Integer, click: String, advertiserID: Integer, browser: String, geo: String, bidAmount: Double)

def GsonTest() : String = {
    val imp = impressions(Random.nextInt(impressions.size)).toSeq
    val dt = new Timestamp(System.currentTimeMillis()-100)

    val r = Record(imp(5).asInstanceOf[String], dt.toString(), imp(0).asInstanceOf[Integer], imp(1).asInstanceOf[String], imp(2).asInstanceOf[Integer], imp(3).asInstanceOf[String], imp(4).asInstanceOf[Integer], imp(6).asInstanceOf[String], imp(7).asInstanceOf[String],imp(8).asInstanceOf[Double])
    // create a JSON string from the Record, then print it
    val gson = new Gson
    val jsonString = gson.toJson(r)
    return jsonString
}


// Generate and send the data
for (round <- 1 to numSecondsToSend) {
  for (recordNum <- 1 to recordsPerSecond) {
    val data = GsonTest()
    println(data)
    val partitionKey = s"partitionKey-$recordNum"
    val putRecordRequest = new PutRecordRequest().withStreamName(kinesisStreamName)
        .withPartitionKey(partitionKey)
        .withData(ByteBuffer.wrap(data.getBytes()))
    kinesisClient.putRecord(putRecordRequest)
    //println(putRecordRequest)
  }
  Thread.sleep(100) // Sleep for a second
  println(s"Sent $recordsPerSecond records with $wordsPerRecord words each")
}

println("\nTotal number of records sent")

// COMMAND ----------

