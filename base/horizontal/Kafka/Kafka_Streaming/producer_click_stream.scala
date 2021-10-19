// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.put("/databricks/init/bk-demo/setup_ssh_tunneling.sh", """#!/bin/bash
// MAGIC 
// MAGIC set -e
// MAGIC 
// MAGIC apt-get install sshpass
// MAGIC 
// MAGIC CLUSTER_SSH_ADDRESS="kafka-azure-ssh.azurehdinsight.net" 
// MAGIC CLUSTER_SSH_USER="sshuser"
// MAGIC CLUSTER_SSH_PASSWORD="Wesley@123"
// MAGIC 
// MAGIC BROKERS=("10.0.0.11:9092" \
// MAGIC "10.0.0.10:9092" \
// MAGIC "10.0.0.13:9092")
// MAGIC 
// MAGIC NUM_BROKERS=${#BROKERS[@]}
// MAGIC NUM_BROKERS_HALF=$(($NUM_BROKERS/2))
// MAGIC 
// MAGIC for (( i=0; i<$NUM_BROKERS; i++ ))
// MAGIC do
// MAGIC 
// MAGIC THIS_BROKER_HOST=${BROKERS[i]}
// MAGIC 
// MAGIC echo "Forwarding $THIS_BROKER_HOST"
// MAGIC 
// MAGIC # register local interface
// MAGIC ip add a dev lo $(echo $THIS_BROKER_HOST | cut -d':' -f 1)
// MAGIC 
// MAGIC TUNNEL_PORT=22
// MAGIC if [ "$i" -ge "$NUM_BROKERS_HALF" ]; then
// MAGIC   # for load balancing between nodes
// MAGIC   TUNNEL_PORT=23
// MAGIC fi
// MAGIC 
// MAGIC # set up ssh tunneling with the broker
// MAGIC sshpass -p $CLUSTER_SSH_PASSWORD ssh -fnNT -o StrictHostKeyChecking=no -p $TUNNEL_PORT -L $THIS_BROKER_HOST:$THIS_BROKER_HOST $CLUSTER_SSH_USER@$CLUSTER_SSH_ADDRESS
// MAGIC 
// MAGIC done
// MAGIC """, True)

// COMMAND ----------

import java.nio.ByteBuffer
import scala.util.Random
import com.google.gson.Gson
import org.apache.kafka.clients.producer._;
import java.util.Properties;
import org.joda.time
import org.joda.time.format._
import java.sql.Timestamp;

new Timestamp(System.currentTimeMillis());

val dt = new Timestamp(System.currentTimeMillis()-100)

// === Configurations of amount of data to produce ===
val recordsPerSecond = 100
val wordsPerRecord = 10
val numSecondsToSend = 6000000

// COMMAND ----------

val rdd = sc.textFile(s"/mnt/wesley/dataset/kafka/click-stream-test/")
rdd.map{line => line}.foreach(println)

// COMMAND ----------

val rdd = sc.textFile(s"/mnt/wesley/dataset/kafka/click-stream-test/000010_0")

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

println(s"Putting records onto stream Kafka Topic at a rate of" +
  s" $recordsPerSecond records per second with $wordsPerRecord words per record for $numSecondsToSend seconds")

 val props = new Properties();
 props.put("bootstrap.servers", "10.0.0.11:9092,10.0.0.10:9092,10.0.0.13:9092");
 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


// Generate and send the data
for (round <- 1 to numSecondsToSend) {
  for (recordNum <- 1 to recordsPerSecond) {
    rdd.foreach(line => {
         val producer = new KafkaProducer[String, String](props);
        val putRecordRequest = new ProducerRecord[String, String]("click-stream-test", line)
        producer.send(putRecordRequest);
        producer.flush()
    })
  }
  Thread.sleep(100) // Sleep for a second
  println(s"Sent $recordsPerSecond records with $wordsPerRecord words each")
}

println("\nTotal number of records sent")
//totals.toSeq.sortBy(_._1).mkString("\n")

// COMMAND ----------

