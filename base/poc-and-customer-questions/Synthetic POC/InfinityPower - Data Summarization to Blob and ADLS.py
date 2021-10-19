# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <script>
# MAGIC       function showhide() {
# MAGIC         var x = document.getElementById("showhide");
# MAGIC         if (x.style.display === "none") {
# MAGIC           x.style.display = "block";
# MAGIC         } else {
# MAGIC           x.style.display = "none";
# MAGIC         }
# MAGIC       }
# MAGIC     </script>
# MAGIC     <style>
# MAGIC       .button {
# MAGIC         background-color: #e7e7e7; color: black;
# MAGIC         border: none;
# MAGIC         color: black;
# MAGIC         padding: 15px 32px;
# MAGIC         text-align: center;
# MAGIC         text-decoration: none;
# MAGIC         display: inline-block;
# MAGIC         font-size: 16px;
# MAGIC         border-radius: 8px;
# MAGIC       }
# MAGIC     </style>
# MAGIC     
# MAGIC   </head>
# MAGIC   
# MAGIC   <body>
# MAGIC      <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC       <img src="files/tables/infinitypower.png" style="width: 700px;">
# MAGIC     </div>
# MAGIC     <div><h3>POC on Databricks</h3></div>
# MAGIC     <p>
# MAGIC       <button class="button" onclick="showhide()">Info on Dataset</button>
# MAGIC     </p>
# MAGIC 
# MAGIC     <div id="showhide" style="display:none">
# MAGIC       <p>
# MAGIC       <h2>Individual household electric power consumption Data Set </h2>
# MAGIC       <a href="http://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption#">From UCI Machine Learning Repository</a>
# MAGIC       <h3>Data Set Information:</h3>
# MAGIC 
# MAGIC       <p>
# MAGIC This archive contains 2075259 measurements gathered in a house located in Sceaux (7km of Paris, France) between December 2006 and November 2010 (47 months). <br>
# MAGIC Notes: <br>
# MAGIC 1.(global_active_power*1000/60 - sub_metering_1 - sub_metering_2 - sub_metering_3) represents the active energy consumed every minute (in watt hour) in the household by electrical equipment not measured in sub-meterings 1, 2 and 3. <br>
# MAGIC 2.The dataset contains some missing values in the measurements (nearly 1,25% of the rows). All calendar timestamps are present in the dataset but for some timestamps, the measurement values are missing: a missing value is represented by the absence of value between two consecutive semi-colon attribute separators. For instance, the dataset shows missing values on April 28, 2007.
# MAGIC </p>
# MAGIC 
# MAGIC       <h3>Attribute Information:</h3>
# MAGIC 
# MAGIC       <p>
# MAGIC 1.date: Date in format dd/mm/yyyy <br>
# MAGIC 2.time: time in format hh:mm:ss <br>
# MAGIC 3.global_active_power: household global minute-averaged active power (in kilowatt) <br>
# MAGIC 4.global_reactive_power: household global minute-averaged reactive power (in kilowatt) <br>
# MAGIC 5.voltage: minute-averaged voltage (in volt) <br>
# MAGIC 6.global_intensity: household global minute-averaged current intensity (in ampere) <br>
# MAGIC 7.sub_metering_1: energy sub-metering No. 1 (in watt-hour of active energy). It corresponds to the kitchen, containing mainly a dishwasher, an oven and a microwave (hot plates are not electric but gas powered). <br>
# MAGIC 8.sub_metering_2: energy sub-metering No. 2 (in watt-hour of active energy). It corresponds to the laundry room, containing a washing-machine, a tumble-drier, a refrigerator and a light. <br>
# MAGIC 9.sub_metering_3: energy sub-metering No. 3 (in watt-hour of active energy). It corresponds to an electric water-heater and an air-conditioner.
# MAGIC       </p>
# MAGIC       </p>
# MAGIC     </div>
# MAGIC   </body>
# MAGIC </html>

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png"> Overview</h2>
# MAGIC 
# MAGIC - Step 1: Business Understanding
# MAGIC - Step 2: Data Access
# MAGIC - Step 3: Data Clearning & Transformation
# MAGIC - Step 4: Exploratory Data Analysis & Visualization
# MAGIC - Step 5: Data Summary
# MAGIC - Step 6: Persist Summary Tables  <br/><br/>
# MAGIC Additional Machine Learning Modeling Steps
# MAGIC - Step 7: Data Preparation
# MAGIC - Step 8: Data Modeling
# MAGIC - Step 9: Tuning and Evaluation
# MAGIC - Step 10: Deployment

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png"> Step 1: Business Understanding</h2>
# MAGIC 
# MAGIC Energy is a fundamental resource that affects every aspect of our lives. We want to be able to generate electric power, right amount at the right time. This is an attempt to study usage pattern among customers to plan for power production.  
# MAGIC Also to potentially to identify which appliances are currently used from global metering data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <script>
# MAGIC       function showhide() {
# MAGIC         var x = document.getElementById("showhide");
# MAGIC         if (x.style.display === "none") {
# MAGIC           x.style.display = "block";
# MAGIC         } else {
# MAGIC           x.style.display = "none";
# MAGIC         }
# MAGIC       }
# MAGIC     </script>
# MAGIC     <style>
# MAGIC       .button {
# MAGIC         background-color: #e7e7e7; color: black;
# MAGIC         border: none;
# MAGIC         color: black;
# MAGIC         padding: 15px 32px;
# MAGIC         text-align: center;
# MAGIC         text-decoration: none;
# MAGIC         display: inline-block;
# MAGIC         font-size: 16px;
# MAGIC         border-radius: 8px;
# MAGIC       }
# MAGIC     </style>
# MAGIC     
# MAGIC   </head>
# MAGIC   
# MAGIC   <body>
# MAGIC     <p>
# MAGIC       <h2><img src="/files/tables/infinitypower_small-ad799.png"> Step 2: Data Access</h2>
# MAGIC     </p>
# MAGIC     
# MAGIC     <p>
# MAGIC       DBFS is a Databricks File System that allows to store data for querying inside of Databricks. It can also be used to map external sources such as data from Blob store or ADLS.
# MAGIC     </p>
# MAGIC   
# MAGIC     <p>
# MAGIC       <button class="button" onclick="showhide()">Mounting Azure Blob Storage & ADLS</button>
# MAGIC     </p>
# MAGIC 
# MAGIC     <div id="showhide" style="display:none">
# MAGIC       <p>
# MAGIC         <h2>Mount Blob Storage
# MAGIC         </h2>
# MAGIC         <code>
# MAGIC           dbutils.fs.mount(<br>
# MAGIC           source = "wasbs://housepowerconsumption@joelsimpleblobstore.blob.core.windows.net/",<br>
# MAGIC           mount_point = "/mnt/joel-blob-poweranalysis",<br>
# MAGIC           extra_configs = {"fs.azure.account.key.joelsimpleblobstore.blob.core.windows.net": accountKey}<br>
# MAGIC           )
# MAGIC         </code>
# MAGIC       </p>
# MAGIC       <p><h2>Mount ADLS</h2>
# MAGIC         <code>
# MAGIC           configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",<br>
# MAGIC           "dfs.adls.oauth2.client.id": applicationId,<br>
# MAGIC           "dfs.adls.oauth2.credential": applicationKey,<br>
# MAGIC           "dfs.adls.oauth2.refresh.url": "https://" + "login.microsoftonline.com/" + directoryId + "/oauth2/token"}<br><br>
# MAGIC           dbutils.fs.mount(<br>
# MAGIC           source = adlUri,<br>
# MAGIC           mount_point = "/mnt/joel-adl-poweranalysis",<br>
# MAGIC           extra_configs = configs<br>
# MAGIC           )
# MAGIC         </code>
# MAGIC       </p>
# MAGIC     </div>
# MAGIC   </body>
# MAGIC </html>

# COMMAND ----------

# DBTITLE 1,Data from Blob Store
display(dbutils.fs.ls("/mnt/joel-blob-poweranalysis/household_power_consumption.txt"))

# COMMAND ----------

# DBTITLE 1,Data pattern in file
# MAGIC %sh head /dbfs/mnt/joel-blob-poweranalysis/household_power_consumption.txt

# COMMAND ----------

# DBTITLE 1,Read raw data to dataframe
from pyspark.sql.types import *

# file_location = "dbfs:/FileStore/tables/household_power_consumption.txt"             # Local Storage
file_location = "dbfs:/mnt/joel-blob-poweranalysis/household_power_consumption.txt"    # Blob Storage

schema = (StructType([
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("global_active_power", DoubleType(), True),
    StructField("global_reactive_power", DoubleType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("global_intensity", DoubleType(), True),
    StructField("sub_metering_1", DoubleType(), True),
    StructField("sub_metering_2", DoubleType(), True),
    StructField("sub_metering_3", DoubleType(), True)])
)

df_raw = (spark.read.csv(file_location, 
                     schema=schema, header=True, 
                     ignoreLeadingWhiteSpace=True, 
                     ignoreTrailingWhiteSpace=True,
                     sep=';')
     )

display(df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png"> Step 3: Data Cleaning & Transformation </h2>

# COMMAND ----------

# DBTITLE 0,Data Cleaning & Transformation
from pyspark.ml.feature import Bucketizer
from pyspark.sql import functions as F

# Compute non sub metered reading
df_transformed = (
       df_raw.
       withColumn("datetime", F.to_timestamp(F.concat_ws(' ', df_raw.date, df_raw.time), 'dd/MM/yyyy HH:mm:ss')).
       withColumn("total_metering", (df_raw.sub_metering_1 + df_raw.sub_metering_2 + df_raw.sub_metering_3))
     )

df_transformed = df_transformed.withColumn("non_sub_metered", ((df_transformed.global_active_power * (1000/60)) - df_transformed.total_metering))

df_transformed = df_transformed.drop("date", "time", "total_metering")


# Create active/inactive tag from meter readings
splits = [-float("inf"), 15, float("inf")]
df_transformed = Bucketizer(splits=splits, inputCol="sub_metering_1", outputCol="sub_metering_1_bucketed").transform(df_transformed)
df_transformed = Bucketizer(splits=splits, inputCol="sub_metering_2", outputCol="sub_metering_2_bucketed").transform(df_transformed)
df_transformed = Bucketizer(splits=splits, inputCol="sub_metering_3", outputCol="sub_metering_3_bucketed").transform(df_transformed)
df_transformed = Bucketizer(splits=splits, inputCol="non_sub_metered", outputCol="non_sub_metered_bucketed").transform(df_transformed)

df_transformed = (
       df_transformed.
       withColumn("sub_metering_1_tag", F.when(df_transformed.sub_metering_1_bucketed == 0, "inactive").otherwise("active")).
       withColumn("sub_metering_2_tag", F.when(df_transformed.sub_metering_2_bucketed == 0, "inactive").otherwise("active")).
       withColumn("sub_metering_3_tag", F.when(df_transformed.sub_metering_3_bucketed == 0, "inactive").otherwise("active")).
       withColumn("non_sub_metered_tag", F.when(df_transformed.non_sub_metered_bucketed == 0, "inactive").otherwise("active")) 
)

df_transformed = (
      df_transformed.
      withColumn("meter_state", F.concat(df_transformed.sub_metering_1_tag.substr(1,1), 
                                      df_transformed.sub_metering_2_tag.substr(1,1),
                                      df_transformed.sub_metering_3_tag.substr(1,1),
                                      df_transformed.non_sub_metered_tag.substr(1,1)))
)

df_transformed = df_transformed.drop("sub_metering_1_bucketed", "sub_metering_2_bucketed", "sub_metering_3_bucketed", "non_sub_metered_bucketed")


display(df_transformed)

# COMMAND ----------

# DBTITLE 1,Temp Table
df_transformed.registerTempTable("power_analysis_transformed")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png"> Step 4: Exploratory Data Analysis & Visualization </h2>

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM power_analysis_transformed

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC power_analysis_transformed

# COMMAND ----------

# DBTITLE 1,Voltage vs Active Power
# MAGIC %sql 
# MAGIC 
# MAGIC SELECT 
# MAGIC   voltage, global_active_power 
# MAGIC FROM 
# MAGIC   power_analysis_transformed

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png"> Step 5: Data Summary </h2>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   to_date(window.start) as window_day, avg_voltage, avg_global_active_power, avg_global_reactive_power, avg_global_intensity, 
# MAGIC   tot_sub_metering_1, tot_sub_metering_2, tot_sub_metering_3, tot_non_sub_metered
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT  
# MAGIC     window(DateTime, '1 day') as window,
# MAGIC     avg(voltage) AS avg_voltage,
# MAGIC     avg(global_active_power) AS avg_global_active_power,
# MAGIC     avg(global_reactive_power) AS avg_global_reactive_power,
# MAGIC     avg(global_intensity) AS avg_global_intensity,
# MAGIC     sum(Sub_metering_1) AS tot_sub_metering_1,
# MAGIC     sum(Sub_metering_2) AS tot_sub_metering_2,
# MAGIC     sum(Sub_metering_3) AS tot_sub_metering_3,
# MAGIC     sum(non_sub_metered) AS tot_non_sub_metered
# MAGIC   FROM 
# MAGIC     power_analysis_transformed
# MAGIC   GROUP BY
# MAGIC     window
# MAGIC   ORDER BY
# MAGIC     window.start ASC
# MAGIC )

# COMMAND ----------

df_summary = spark.sql(
  """SELECT 
    to_date(window.start) as window_day, avg_voltage, avg_global_active_power, avg_global_reactive_power, avg_global_intensity, 
    tot_sub_metering_1, tot_sub_metering_2, tot_sub_metering_3, tot_non_sub_metered
  FROM
  (
    SELECT  
      window(DateTime, '1 day') as window,
      avg(voltage) AS avg_voltage,
      avg(global_active_power) AS avg_global_active_power,
      avg(global_reactive_power) AS avg_global_reactive_power,
      avg(global_intensity) AS avg_global_intensity,
      sum(Sub_metering_1) AS tot_sub_metering_1,
      sum(Sub_metering_2) AS tot_sub_metering_2,
      sum(Sub_metering_3) AS tot_sub_metering_3,
      sum(non_sub_metered) AS tot_non_sub_metered
    FROM 
      power_analysis_transformed
    GROUP BY
      window
    ORDER BY
      window.start ASC
  )"""
)

# COMMAND ----------

display(df_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png"> Step 6: Persist Summary Tables </h2>

# COMMAND ----------

# DBTITLE 1,Persist Parsed Data in Blob Store - Parquet
spark.sql("DROP TABLE IF EXISTS power_analysis")
df_transformed.write.saveAsTable("power_analysis", format = "parquet", mode = "overwrite", path = "/mnt/joel-blob-poweranalysis/parsed")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <script>
# MAGIC       function showhide(name) {
# MAGIC         var x = document.getElementById(name);
# MAGIC         if (x.style.display === "none") {
# MAGIC           x.style.display = "block";
# MAGIC         } else {
# MAGIC           x.style.display = "none";
# MAGIC         }
# MAGIC       }
# MAGIC     </script>
# MAGIC     <style>
# MAGIC       .button {
# MAGIC         background-color: #e7e7e7; color: black;
# MAGIC         border: none;
# MAGIC         color: black;
# MAGIC         padding: 15px 32px;
# MAGIC         text-align: center;
# MAGIC         text-decoration: none;
# MAGIC         display: inline-block;
# MAGIC         font-size: 16px;
# MAGIC         border-radius: 8px;
# MAGIC       }
# MAGIC     </style>
# MAGIC     
# MAGIC   </head>
# MAGIC   
# MAGIC   <body>
# MAGIC     <p>
# MAGIC       <button class="button" onclick="showhide('delta')">Delta</button>
# MAGIC       <button class="button" onclick="showhide('reliable')">Reliable</button>
# MAGIC       <button class="button" onclick="showhide('performant')">Performant</button>
# MAGIC     </p>
# MAGIC 
# MAGIC     <div id="delta" style="display:none">
# MAGIC       <p>
# MAGIC       <img src="files/tables/Delta_Make_Data_Ready_For_Analytics-db9df.png" style="width: 1200px;">
# MAGIC       </p>
# MAGIC       <div><img src="https://files.training.databricks.com/images/eLearning/Delta/delta.png" style="height: 350px"/></div>
# MAGIC     </div>
# MAGIC     <div id="reliable" style="display:none">
# MAGIC       <p>
# MAGIC       <img src="files/tables/Delta_Without_Reliability_Challenges.png" style="width: 1200px;">
# MAGIC       </p>
# MAGIC       <p>
# MAGIC       <img src="files/tables/Delta_Reliable.png" style="width: 1200px;">
# MAGIC       </p>
# MAGIC     </div>
# MAGIC     <div id="performant" style="display:none">
# MAGIC       <p>
# MAGIC       <img src="files/tables/Delta_Without_Performance_Challenges.png" style="width: 1200px;">
# MAGIC       </p>
# MAGIC       <p>
# MAGIC       <img src="files/tables/Delta_Performant.png" style="width: 1200px;">
# MAGIC       </p>
# MAGIC     </div>
# MAGIC   </body>
# MAGIC </html>

# COMMAND ----------

# DBTITLE 1,Persist Parsed Data in Blob Store - Delta
spark.sql("DROP TABLE IF EXISTS power_analysis")
df_transformed.write.option("overwriteSchema", "true").saveAsTable("power_analysis", format = "delta", mode = "overwrite", path = "/mnt/joel-blob-poweranalysis/parsed-delta")

# COMMAND ----------

# DBTITLE 1,Persist Summary Data in ADLS
spark.sql("DROP TABLE IF EXISTS power_analysis_summary")
df_summary.write.saveAsTable("power_analysis_summary", format = "delta", mode = "overwrite", path = "/mnt/joel-adl-poweranalysis/power-analysis-summary")

# COMMAND ----------

