# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # Integration Tests
# MAGIC The purpose of this notebook is to faciliate testing of our systems.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #Nothing to see here...
# MAGIC <div style="margin-top:1em">
# MAGIC   <img src="https://files.training.databricks.com/images/nothing-to-see-here.jpg" style="max-height: 400px">
# MAGIC </div>
# MAGIC #Move along...

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC course_name = "Integration Tests"

# COMMAND ----------

# MAGIC %run ./Dataset-Mounts

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val testStart = System.currentTimeMillis
# MAGIC 
# MAGIC val mountPointBase = "/mnt/training-test"
# MAGIC val regions = scala.collection.mutable.ArrayBuffer[String]()
# MAGIC 
# MAGIC def unmount(mountPoint:String):Unit = {
# MAGIC   try {
# MAGIC     dbutils.fs.unmount(mountPoint)
# MAGIC   } catch {
# MAGIC     case e:Exception => println(s"Not mounted: $mountPoint")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def testRegion(regionType:String, regionName:String, mapper: (String) => (String,Map[String,String])):Unit = {
# MAGIC   regions.append(regionName)
# MAGIC 
# MAGIC   val start = System.currentTimeMillis
# MAGIC   
# MAGIC   val (source, extraConfigs) = mapper(regionName)
# MAGIC   val mountPoint = s"$mountPointBase-${regionType.toLowerCase}-$regionName"
# MAGIC   println(s"""Testing the $regionType region $regionName ($mountPoint)""")
# MAGIC 
# MAGIC   mountSource(true, false, mountPoint, source, extraConfigs)
# MAGIC   validateDatasets(mountPoint)
# MAGIC   
# MAGIC   val duration = (System.currentTimeMillis - start) / 1000.0
# MAGIC   println(f"...all tests passed in $duration%1.2f seconds!")
# MAGIC }
# MAGIC 
# MAGIC def validateDataset(mountPoint:String, target:String):Unit = {
# MAGIC   val map = scala.collection.mutable.Map[String,(Long,Long)]()
# MAGIC   for (file <- dbutils.fs.ls(s"/mnt/training/$target")) {
# MAGIC     map.put(file.name, (file.size, -1L))
# MAGIC   }
# MAGIC 
# MAGIC   val path = s"$mountPoint/$target"
# MAGIC   for (file <- dbutils.fs.ls(path)) {
# MAGIC       if (map.contains(file.name)) {
# MAGIC         val (sizes, _) = map(file.name)
# MAGIC         map.put(file.name, (sizes, file.size))
# MAGIC       } else {
# MAGIC         map.put(file.name, (-1, file.size))
# MAGIC       }
# MAGIC   }
# MAGIC   
# MAGIC   var errors = ""
# MAGIC   for (key <- map.keySet) {
# MAGIC     val (sizeA, sizeB) = map(key)
# MAGIC     if (sizeA == sizeB) {
# MAGIC       // Everything matches up... no issue here.
# MAGIC     } else if (sizeA == -1) {
# MAGIC       errors += s"Extra file: $path$key\n"
# MAGIC     } else if (sizeB == -1) {
# MAGIC       errors += s"Missing file: $path$key\n"
# MAGIC     }
# MAGIC   }
# MAGIC   
# MAGIC   errors = errors.trim()
# MAGIC   if (errors != "") {
# MAGIC     println(errors)
# MAGIC     throw new IllegalStateException(s"Errors were found while processing $path")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC 
# MAGIC def validateDatasets(mountPoint:String) {
# MAGIC   val paths = List(
# MAGIC     "",
# MAGIC     "301/",
# MAGIC     "Chicago-Crimes-2018.csv",
# MAGIC     "City-Data.parquet/",
# MAGIC     "EDGAR-Log-20170329/",
# MAGIC     "UbiqLog4UCI/",
# MAGIC     "_META/",
# MAGIC     "adventure-works/",
# MAGIC     "airbnb/",
# MAGIC     "airbnb-sf-listings.csv",
# MAGIC     "asa/",
# MAGIC     "auto-mpg.csv",
# MAGIC     "bigrams/",
# MAGIC     "bikeSharing/",
# MAGIC     "bostonhousing/",
# MAGIC     "cancer/",
# MAGIC     "countries/",
# MAGIC     "crime-data-2016/",
# MAGIC     "data/",
# MAGIC     "data-cleansing/",
# MAGIC     "databricks-blog.json",
# MAGIC     "databricks-datasets/",
# MAGIC     "dataframes/",
# MAGIC     "day-of-week/",
# MAGIC     "definitive-guide/",
# MAGIC     "dl/",
# MAGIC     "gaming_data/",
# MAGIC     "global-sales/",
# MAGIC     "graphx-demo/",
# MAGIC     "initech/",
# MAGIC     "ip-geocode.parquet/",
# MAGIC     "iris/",
# MAGIC     "mini_newsgroups/",
# MAGIC     "mnist/",
# MAGIC     "movie-reviews/",
# MAGIC     "movielens/",
# MAGIC     "movies/",
# MAGIC     "online_retail/",
# MAGIC     "philadelphia-crime-data-2015-ytd.csv",
# MAGIC     "purchases.txt",
# MAGIC     "sensor-data/",
# MAGIC     "ssn/",
# MAGIC     "stopwords",
# MAGIC     "structured-streaming/",
# MAGIC     "test.log",
# MAGIC     "tom-sawyer/",
# MAGIC     "tweets.txt",
# MAGIC     "twitter/",
# MAGIC     "wash_dc_crime_incidents_2013.csv",
# MAGIC     "wash_dc_crime_incidents_2015-10-03-to-2016-10-02.csv",
# MAGIC     "weather/",
# MAGIC     "wikipedia/",
# MAGIC     "wine.parquet/",
# MAGIC     "word-game-dict.txt",
# MAGIC     "zip3state.csv",
# MAGIC     "zips.json"
# MAGIC   )
# MAGIC   for (path <- paths) {
# MAGIC     validateDataset(mountPoint, path)
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "us-west-2", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "ap-northeast-2", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "ap-south-1", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "ap-southeast-1", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "ap-southeast-2", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "ca-central-1", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "eu-central-1", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "eu-west-1", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "eu-west-2", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "eu-west-3", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "sa-east-1", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "us-east-1", getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("AWS", "us-east-2",  getAwsMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("Azure", "eastasia", getAzureMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("Azure", "eastus", getAzureMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("Azure", "eastus2", getAzureMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("Azure", "northcentralus", getAzureMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("Azure", "northeurope", getAzureMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("Azure", "southcentralus", getAzureMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("Azure", "southeastasia", getAzureMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("Azure", "westeurope", getAzureMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("Azure", "westus", getAzureMapping _)

# COMMAND ----------

# MAGIC %scala
# MAGIC testRegion("Azure", "westus2", getAzureMapping _)