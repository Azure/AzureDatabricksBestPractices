# Databricks notebook source
# DBTITLE 1,Stream Processing
# MAGIC %md 
# MAGIC ![Architecture](https://s3-us-west-1.amazonaws.com/databricks-binu-mathew/image/9.png)

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC 
# MAGIC set -e
# MAGIC 
# MAGIC apt-get install sshpass
# MAGIC 
# MAGIC CLUSTER_SSH_ADDRESS="kafka-azure-ssh.azurehdinsight.net" 
# MAGIC CLUSTER_SSH_USER="sshuser"
# MAGIC CLUSTER_SSH_PASSWORD="Wesley@123"
# MAGIC 
# MAGIC BROKERS=("10.0.0.11:9092" \
# MAGIC "10.0.0.10:9092" \
# MAGIC "10.0.0.13:9092")
# MAGIC 
# MAGIC NUM_BROKERS=${#BROKERS[@]}
# MAGIC NUM_BROKERS_HALF=$(($NUM_BROKERS/2))
# MAGIC 
# MAGIC for (( i=0; i<$NUM_BROKERS; i++ ))
# MAGIC do
# MAGIC 
# MAGIC THIS_BROKER_HOST=${BROKERS[i]}
# MAGIC 
# MAGIC echo "Forwarding $THIS_BROKER_HOST"
# MAGIC 
# MAGIC # register local interface
# MAGIC ip add a dev lo $(echo $THIS_BROKER_HOST | cut -d':' -f 1)
# MAGIC 
# MAGIC TUNNEL_PORT=22
# MAGIC if [ "$i" -ge "$NUM_BROKERS_HALF" ]; then
# MAGIC   # for load balancing between nodes
# MAGIC   TUNNEL_PORT=23
# MAGIC fi
# MAGIC 
# MAGIC # set up ssh tunneling with the broker
# MAGIC sshpass -p $CLUSTER_SSH_PASSWORD ssh -fnNT -o StrictHostKeyChecking=no -p $TUNNEL_PORT -L $THIS_BROKER_HOST:$THIS_BROKER_HOST $CLUSTER_SSH_USER@$CLUSTER_SSH_ADDRESS
# MAGIC 
# MAGIC done

# COMMAND ----------

# DBTITLE 1,Connect to Kafka Cluster and Stream JSON data from Topic
## connect to a Kafka cluster and subscribe to a topic

kafkaDF1 = spark.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "10.0.0.11:9092,10.0.0.10:9092,10.0.0.13:9092") \
.option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
.option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
.option("subscribe", "click-stream-test") \
.option("startingOffsets", "earliest") \
.option("failOnDataLoss", "true") \
.load() 

# COMMAND ----------

# DBTITLE 1,Parse a few fields from JSON Stream 
## parse JSON fields from stream and store into DataFrame and a Temporary Table
from pyspark.sql.functions import get_json_object

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import unix_timestamp

streamingDF = kafkaDF1.select(kafkaDF1.value.cast('string')) 

parsedFieldsDF = streamingDF.select(get_json_object('value', '$.http_vhost_name').alias('domain_name') \
    ,regexp_replace(regexp_replace(get_json_object('value', '$.event_time'),'T',' '),'Z','').alias('event_time') \
    ,unix_timestamp(regexp_replace(regexp_replace(get_json_object('value', '$.event_time'),'T',' '),'Z','')).alias('unix_timestamp') \
    ,get_json_object('value', '$.page.country').alias('page_country') \
    ,get_json_object('value', '$.page.url').alias('page_url') \
    ,get_json_object('value', '$.session.referral.url').alias('referrer_url') \
    ,get_json_object('value', '$.user.browser').alias('browser') \
    ,get_json_object('value', '$.user.browserId').alias('user_cookie') \
    ,get_json_object('value', '$.user.os').alias('os') \
    ,get_json_object('value', '$.user.platform').alias('platform')    
).where("page_country IN ('MX','TW','PE','CO','UY','PR','CL','ID','CA','GB','JP','US','PA')")

parsedFieldsDF.registerTempTable("clickstream_hits")
                                                      

# COMMAND ----------

# DBTITLE 1,Display Streaming Data in DataFrame
display(parsedFieldsDF)

# COMMAND ----------

# DBTITLE 1,Top Operating Systems by Page Views
from pyspark.sql.functions import desc

topOsDF = parsedFieldsDF.select('os') \
.where("os NOT LIKE '%\%%'") \
.groupBy('os') \
.count() 

display(topOsDF)

# COMMAND ----------

# DBTITLE 1,Unique Visitors By Country
# MAGIC %sql
# MAGIC SELECT page_country, approx_count_distinct(user_cookie) AS unique_users FROM clickstream_hits GROUP BY page_country

# COMMAND ----------

# DBTITLE 1,Page Views by Country for 1 minute window interval with sliding duration of 30 seconds
from pyspark.sql.functions import window
from pyspark.sql.functions import asc

queryDF = parsedFieldsDF.select('page_country','event_time') \
.groupBy('page_country', window('event_time', '1 minute' , '30 second')) \
.count() \
.orderBy(asc("window"))

display(queryDF)

# COMMAND ----------

# DBTITLE 1,Join streaming DataFrame with static table for data enrichment
## the streaming data frame contains 2 letter country codes
## join this country code to a country code lookup table to get the ISO country code

countryLkpDF = spark.table("bmathew.country_lkp").cache()
joinDF = parsedFieldsDF.join(countryLkpDF, "page_country")

display(joinDF)

# COMMAND ----------

# DBTITLE 1,Persist Streaming DataFrame to Parquet File Format in S3
#from pyspark.sql.streaming import ProcessingTime

parsedFieldsDF.writeStream \
  .format("parquet") \
  .option("path", "/kafka-test/data") \
  .option("checkpointLocation", "/kafka-test/checkpoint") \
  .trigger(processingTime="1 milliseconds") \
  .start()


# COMMAND ----------

# DBTITLE 1,Create Hive table over Parquet files (Connect BI Tool to this Hive Table)
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS clickstream_traffic; 
# MAGIC 
# MAGIC CREATE EXTERNAL TABLE clickstream_traffic (
# MAGIC domain_name string
# MAGIC ,event_time string
# MAGIC ,unix_timestamp bigint
# MAGIC ,page_country string
# MAGIC ,page_url string
# MAGIC ,referrer_url string
# MAGIC ,browser string
# MAGIC ,user_cookie string
# MAGIC ,os string
# MAGIC ,platform string)
# MAGIC STORED AS PARQUET
# MAGIC LOCATION '/kafka-test/data/'

# COMMAND ----------

# DBTITLE 1,Query Data in Hive Table (Connect BI Tool to this Hive Table)
# MAGIC %sql
# MAGIC SELECT domain_name, count(*) FROM clickstream_traffic group by domain_name 