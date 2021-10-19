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

// MAGIC %run ./profileHelper

// COMMAND ----------

// === Configurations for Kinesis streams ===
val aws_kinesis_keys = get_kinesis_creds
val kinesisStreamName = get_kinesis_stream
val kinesisEndpointUrl = get_kinesis_endpoint() // e.g. https://kinesis.us-west-2.amazonaws.com"

// === Configurations of amount of data to produce ===
val recordsPerSecond = 3
val wordsPerRecord = 10
val numSecondsToSend = 6000000

// COMMAND ----------

// MAGIC %sql describe paul.people_profiles

// COMMAND ----------

// Create the low-level Kinesis Client from the AWS Java SDK.
val kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(aws_kinesis_keys(0), aws_kinesis_keys(1)))
kinesisClient.setEndpoint(kinesisEndpointUrl)

println(s"Putting records onto stream $kinesisStreamName and endpoint $kinesisEndpointUrl at a rate of" +
  s" $recordsPerSecond records per second with $wordsPerRecord words per record for $numSecondsToSend seconds")

// Function to generate data

val conversions = spark.sql("select * from paul.people_profiles").collect()

case class Conversion(timestamp: String, uid: String, street: String, city: String, state: String, zipcode: String, firstname: String, lastname: String)

def GsonTest() : String = {
    val conv = conversions(Random.nextInt(conversions.size)).toSeq
    val dt = new Timestamp(System.currentTimeMillis()-100)
  
    val c = Conversion(dt.toString(), conv(0).asInstanceOf[String], conv(1).asInstanceOf[String], conv(2).asInstanceOf[String], conv(3).asInstanceOf[String], conv(4).asInstanceOf[String], conv(5).asInstanceOf[String], conv(6).asInstanceOf[String])
    // create a JSON string from the Conversion, then print it
  
    val gson = new Gson
    val jsonString = gson.toJson(c)
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
    println(putRecordRequest)
  }
  Thread.sleep(600) // Sleep for a second
  println(s"Sent $recordsPerSecond records with $wordsPerRecord words each")
}

println("\nTotal number of records sent")

// COMMAND ----------

