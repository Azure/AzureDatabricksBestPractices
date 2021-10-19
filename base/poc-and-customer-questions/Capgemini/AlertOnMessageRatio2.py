# Databricks notebook source
import os
from pyspark.sql.functions import col,count, max, expr, lit, sum, to_date, window
from pyspark.sql import Row, Window

# COMMAND ----------

dfRate=(spark.readStream
          .format("rate")
          .option("rowsPerSecond",100)
          .load()
          .withColumn("TraderID",col("value") % 10)
       )

# COMMAND ----------

dfRateAggregated=(dfRate
                  .withWatermark("Timestamp", "30 minutes")
                  .groupBy("TraderID",window("Timestamp","1 days", "1 days"))
                  .agg(count(col("TraderID")).alias("MessageCount"),max(col("Timestamp")).alias("DateTime"))
                  .withColumn("Date",to_date("DateTime", "yyyy-MM-dd"))
)

# COMMAND ----------

onePartitionWindow=Window.partitionBy()

# COMMAND ----------

def fctAlertByRow(row):
  if (
                  int(row["MessageCount"]) >= 0.1 * int(row["TotalMessageCount"]) 
                    and os.path.isdir("/dbfs/FileStore/Alerts/"+ str(row["Date"]) +"/" + str(row["TraderID"]))==False
  ):
    #Some alert code based on group count, total count and total count without TraderID zero
    os.makedirs(                                      # Code to mark alerts
                    "/dbfs/FileStore/Alerts/"         # which have been
                      + str(row["Date"])              # already
                      +"/"                            # send
                      + str(row["TraderID"])          # 
    )  
  pass

# COMMAND ----------

def fctMicroBatchProcessing(dfMicroBatch,microBatchId):
  dfMicroBatchWithTotal=dfMicroBatch.withColumn("TotalMessageCount", sum("MessageCount").over(onePartitionWindow)) #Total
  (
    dfMicroBatchWithTotal.where("TraderID <> 0")
                         .withColumn("UniperMessageCount", sum("MessageCount").over(onePartitionWindow)) #Total for non zero TraderIDs
                         .foreach(fctAlertByRow)
  )
  pass

# COMMAND ----------

qyAlert=(dfRateAggregated.writeStream
                 .outputMode("complete")  #Run in complete mode, update mode possible?
                 .foreachBatch(fctMicroBatchProcessing)
                 .start()
)

qyAlert.awaitTermination()

# COMMAND ----------

