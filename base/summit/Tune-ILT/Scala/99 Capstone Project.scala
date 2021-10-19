// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Capstone Project
// MAGIC 
// MAGIC **Dataset:**
// MAGIC * This lab uses a synthetic data generated specifically for these exercises
// MAGIC * Each year's of data is roughly the same with some variation for market growth in terms of sales volume
// MAGIC * We are looking at retail purchases from the top 100 retailers

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Your Personal Cluster
// MAGIC 
// MAGIC If you cluster already exists, just verify the settings are correct - edit & restart if necessary.
// MAGIC 
// MAGIC If your cluster does not yet exist, create your cluster as outlined below:

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC **Standard Configuration:**
// MAGIC * **Cluster Name**: Use your first name or pick a nickname for yourself. Avoid initials.
// MAGIC * **Cluster Mode**: Select <b style="color:blue">Standard</b>
// MAGIC * **Databricks Runtime Version**: Select the latest version, **unless instructed otherwise**
// MAGIC * **Python Version**: Select Python <b style="color:blue">3</b>
// MAGIC * **Driver Type**: Select <b style="color:blue">Same as worker</b>
// MAGIC * **Worker Type**: Select <b style="color:blue">Standard_D3_v2</b>.
// MAGIC   * <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The default value looks very similar!
// MAGIC * **Enable autoscaling**: <b style="color:blue">Disable</b>, or rather, uncheck
// MAGIC * **Workers**: Please select only <b style="color:blue">2</b> workers. Selecting more will prevent the labs from functioning properly
// MAGIC * **Auto Termination**: <b style="color:blue">120 minutes</b>
// MAGIC 
// MAGIC This should yield a <b style="color:blue">42 GB</b> cluster with <b style="color:blue">8 cores</b>.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cells to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %run "../Includes/Initialize-Labs"

// COMMAND ----------

// MAGIC %run "./Includes/Utility-Methods"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Ready to go?
// MAGIC 
// MAGIC Let's make sure we are running with the expected 8 cores:

// COMMAND ----------

clearYourResults()
validateYourAnswer("00) Only 8 Cores", expectedHash=1276280174, answer=sc.defaultParallelism)
summarizeYourResults()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Introduction
// MAGIC 
// MAGIC We have [fictional] retail data inclusive of the years 2011 through 2018.
// MAGIC 
// MAGIC The data has its origins in a relational database. As a consequence we have one table per year, and a couple of lookup tables:
// MAGIC 
// MAGIC Years 2011 to 2018 were each processed by different individuals resulting in slightly different formats. For example:
// MAGIC * 2012 was written as a CSV
// MAGIC * 2017 was not partitioned at all
// MAGIC * 2014 and 2018 however, were partitioned by year and month
// MAGIC 
// MAGIC These files can be found in **"/mnt/training/global-sales/transactions"**:
// MAGIC * **dbfs:/mnt/training/global-sales/transactions/2011.parquet/**
// MAGIC * **dbfs:/mnt/training/global-sales/transactions/2012.csv/**
// MAGIC * **dbfs:/mnt/training/global-sales/transactions/2013.parquet/**
// MAGIC * **dbfs:/mnt/training/global-sales/transactions/2014.parquet/**
// MAGIC * **dbfs:/mnt/training/global-sales/transactions/2015.parquet/**
// MAGIC * **dbfs:/mnt/training/global-sales/transactions/2016.parquet/**
// MAGIC * **dbfs:/mnt/training/global-sales/transactions/2017.parquet/**
// MAGIC * **dbfs:/mnt/training/global-sales/transactions/2018.parquet/**
// MAGIC 
// MAGIC In addition to the transactional data, we have two lookup tables consisting of retailer and location information.
// MAGIC * **dbfs:/mnt/training/global-sales/cities/all.parquet/**	
// MAGIC * **dbfs:/mnt/training/global-sales/retailers/all.parquet/**
// MAGIC 
// MAGIC The goal of this exercise is two-part:
// MAGIC   0. Answer some business questions about the 2011 to 2018 data.
// MAGIC   0. Clean up the data for future analysis with an eye to 2019 and 2020's data.
// MAGIC     * Optimize the data for better query performance
// MAGIC     * Create utility methods for code reuse
// MAGIC     
// MAGIC **Note:** The sequence of events outlined in this module are not necessarily the most efficient, just logical.   
// MAGIC It's up to you to implement the necessary shortcuts required to effectively work with "big data".

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
// MAGIC 
// MAGIC The first task is to load the schema:
// MAGIC * For each year of data, declare a DataFrame that reads in the Parquet or CSV file.
// MAGIC * The variable name for each DataFrame should take the form of **initDF_*YY*** where * **YY** * is the last two digits of the year.

// COMMAND ----------

// TODO

import FILL_IN

val initDF_11 = FILL_IN
val initDF_12 = FILL_IN
val initDF_13 = FILL_IN
val initDF_14 = FILL_IN
val initDF_15 = FILL_IN
val initDF_16 = FILL_IN
val initDF_17 = FILL_IN
val initDF_18 = FILL_IN

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1
// MAGIC 
// MAGIC To validate your work, run the following cell.
// MAGIC 
// MAGIC **Note:** This is just here to help keep you on track.

// COMMAND ----------

clearYourResults()

validateYourAnswer("01.A) DataFrame 2011", 1929623325, initDF_11.columns.size)
validateYourAnswer("01.B) DataFrame 2012", 1929623325, initDF_12.columns.size)
validateYourAnswer("01.C) DataFrame 2013", 1929623325, initDF_13.columns.size)
validateYourAnswer("01.D) DataFrame 2014", 1276280174, initDF_14.columns.size)
validateYourAnswer("01.E) DataFrame 2015", 1573909955, initDF_15.columns.size)
validateYourAnswer("01.F) DataFrame 2016", 1929623325, initDF_16.columns.size)
validateYourAnswer("01.G) DataFrame 2017", 1929623325, initDF_17.columns.size)
validateYourAnswer("01.H) DataFrame 2018", 1276280174, initDF_18.columns.size)

summarizeYourResults()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #2
// MAGIC 
// MAGIC **The next task is to load the schema for retailers and cities:**
// MAGIC * Read in the parquet file **dbfs:/mnt/training/global-sales/cities/all.parquet/** and assign it to **citiesDF**. 
// MAGIC * Read in the parquet file **dbfs:/mnt/training/global-sales/retailers/all.parquet/** and assign it to **retailersDF**.
// MAGIC 
// MAGIC **But there's a problem with our data:**
// MAGIC * Take a look at the distinct list of countries.
// MAGIC * You should see that one country in particular is in there twice
// MAGIC   * Once fully spelled out
// MAGIC   * Once as an abbreviation
// MAGIC   * **Hint:** It's the "land of the free and the home of the brave"
// MAGIC 
// MAGIC **Clean up the data:**
// MAGIC * Pick one of the two values (full name or abbreviation)
// MAGIC * Update all the [incorrect] records to contain only one of the two values

// COMMAND ----------

// TODO

val citiesDF = FILL_IN
val retailersDF = FILL_IN

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #2
// MAGIC 
// MAGIC To validate your work, run the following cell
// MAGIC 
// MAGIC **Note:** This is just here to help keep you on track.

// COMMAND ----------

clearYourResults()

validateYourAnswer("02.A) Cities Column Count", 135093849, citiesDF.columns.size)
validateYourAnswer("02.B) Cities Column", 646192812, citiesDF.columns.contains("state"))

validateYourAnswer("02.C) Retailers Column Count", 1929623325, retailersDF.columns.size)
validateYourAnswer("02.D) Retailers Column", 646192812, retailersDF.columns.contains("retailer"))

val usaCount = Math.max(citiesDF.filter($"country" === "United States").count(),
                        citiesDF.filter($"country" === "USA").count())
validateYourAnswer("02.E) Expected 73883 US Cities", 1813573044, usaCount)

summarizeYourResults()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #3
// MAGIC 
// MAGIC It can be assumed that every dataset includes the "standard" columns:
// MAGIC * **transacted_at**
// MAGIC * **trx_id**
// MAGIC * **retailer_id**
// MAGIC * **description**
// MAGIC * **amount**
// MAGIC * **city_id**
// MAGIC 
// MAGIC However the datasets may vary from year to year.
// MAGIC 
// MAGIC For example:
// MAGIC * One dataset might include only the "standard" columns while another might add **year** and **month**. 
// MAGIC * One dataset might represent the **amount** as a string while another represents it as a decimal.
// MAGIC 
// MAGIC **Create a function to standardize each dataset:**
// MAGIC * Name the function **standardizeSchema**
// MAGIC * The function should take a single parameter of type **DataFrame**
// MAGIC * The function should return a **DataFrame**
// MAGIC * Drop any extra columns that might exist - (less data == faster processing)
// MAGIC * All 8 datasets should conform to the exact same schema
// MAGIC * Do not rename any of the columns loaded from Parquet
// MAGIC 
// MAGIC **Warning:** Make sure not to modify any DataFrame unnecessarily.  
// MAGIC This might cripple the Catalyst Optimizer, namely Predicate Pushdowns.  
// MAGIC This can be verified by examining the physical plan.

// COMMAND ----------

// TODO

def standardizeSchema(FILL_IN):FILL_IN = {
  FILL_IN
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #3
// MAGIC 
// MAGIC To validate your work, run the following cell
// MAGIC 
// MAGIC **Note:** This is just here to help keep you on track.

// COMMAND ----------

def validateTrxSchema(year:Any, df:DataFrame):Unit = {
  validateYourAnswer(s"03.$year.A) Has 6 columns", 1929623325, df.columns.size)
  
  val schemaStr = df.schema.mkString("|") 
  validateYourAnswer(s"03.$year.B) Contains transacted_at", 646192812, schemaStr.contains("transacted_at,TimestampType"))
  validateYourAnswer(s"03.$year.C) Contains trx_id", 646192812, schemaStr.contains("trx_id,IntegerType"))
  validateYourAnswer(s"03.$year.D) Contains retailer_id", 646192812, schemaStr.contains("retailer_id,IntegerType"))
  validateYourAnswer(s"03.$year.E) Contains description", 646192812, schemaStr.contains("description,StringType"))
  validateYourAnswer(s"03.$year.F) Contains amount", 646192812, schemaStr.contains("amount,DecimalType"))
  validateYourAnswer(s"03.$year.G) Contains city_id", 646192812, schemaStr.contains("city_id,IntegerType"))
}

// COMMAND ----------

clearYourResults()
validateTrxSchema(2011, standardizeSchema(initDF_11))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateTrxSchema(2012, standardizeSchema(initDF_12))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateTrxSchema(2013, standardizeSchema(initDF_13))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateTrxSchema(2014, standardizeSchema(initDF_14))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateTrxSchema(2015, standardizeSchema(initDF_15))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateTrxSchema(2016, standardizeSchema(initDF_16))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateTrxSchema(2017, standardizeSchema(initDF_17))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateTrxSchema(2018, standardizeSchema(initDF_18))
summarizeYourResults()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #4
// MAGIC 
// MAGIC Our data exists in two lookup tables (retailer and city) and one additional table (transactions) per year.
// MAGIC 
// MAGIC That means if we want to pull in data such as the retailer name or the city and state, we have to do a join across 3 tables.
// MAGIC 
// MAGIC Joins of this type can be really expensive for consecutive queries.
// MAGIC 
// MAGIC To help optimize for future queries, we need to denormalization our data and then save that off for future use.
// MAGIC 
// MAGIC **Create a function to denormalize a dataset by joining retailers, cities, and transactions:**
// MAGIC * Name the function **denormalize**
// MAGIC * The function should take a single parameter of type **DataFrame**
// MAGIC * The function should return a **DataFrame** which is a join of **initDF_all**, **retailersDF** and **citiesDF**
// MAGIC * The function should drop all the unnecessary columns: 
// MAGIC   * **city_id**
// MAGIC   * **retailer_id** 
// MAGIC   * **trx_id**
// MAGIC   * **us_sales**
// MAGIC   * **other_sales**
// MAGIC   * **all_sales**
// MAGIC   * **us_vs_world**
// MAGIC   
// MAGIC * **Hint #1:** Dropping the column at the right time can significantly increase performance of the join.
// MAGIC * **Hint #2:** Consider the type of join being executed to futher increase performance.

// COMMAND ----------

// TODO

def denormalize(FILL_IN):FILL_IN = {
  FILL_IN
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #4
// MAGIC 
// MAGIC To validate your work, run the following cell.
// MAGIC 
// MAGIC **Note:** This is just here to help keep you on track.

// COMMAND ----------

def validateDenormalize(year:Int, df:DataFrame):Unit = {
  validateYourAnswer(s"04.$year.A) Has 8 columns", 1276280174, df.columns.size)

  val schemaStr = df.schema.mkString("|") 
  validateYourAnswer(s"04.$year.B) Contains transacted_at [Timestamp]", 646192812, schemaStr.contains("transacted_at,TimestampType"))
  validateYourAnswer(s"04.$year.C) Contains description [String]", 646192812, schemaStr.contains("description,StringType"))
  validateYourAnswer(s"04.$year.D) Contains amount [Decimal]", 646192812, schemaStr.contains("amount,DecimalType"))
  validateYourAnswer(s"04.$year.E) Contains city [String]", 646192812, schemaStr.contains("city,StringType"))
  validateYourAnswer(s"04.$year.F) Contains state [State]", 646192812, schemaStr.contains("state,StringType"))
  validateYourAnswer(s"04.$year.G) Contains state_abv [String]", 646192812, schemaStr.contains("state_abv,StringType"))
  validateYourAnswer(s"04.$year.H) Contains country [String]", 646192812, schemaStr.contains("country,StringType"))
  validateYourAnswer(s"04.$year.I) Contains retailer [String]", 646192812, schemaStr.contains("retailer,StringType"))
}

clearYourResults()

// COMMAND ----------

clearYourResults()
validateDenormalize(2011, denormalize(standardizeSchema(initDF_11)))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateDenormalize(2012, denormalize(standardizeSchema(initDF_12)))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateDenormalize(2013, denormalize(standardizeSchema(initDF_13)))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateDenormalize(2014, denormalize(standardizeSchema(initDF_14)))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateDenormalize(2015, denormalize(standardizeSchema(initDF_15)))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateDenormalize(2016, denormalize(standardizeSchema(initDF_16)))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateDenormalize(2017, denormalize(standardizeSchema(initDF_17)))
summarizeYourResults()

// COMMAND ----------

clearYourResults()
validateDenormalize(2018, denormalize(standardizeSchema(initDF_18)))
summarizeYourResults()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #5
// MAGIC 
// MAGIC One of our tasks is to write the entire dataset out to disk.
// MAGIC 
// MAGIC If we write the data out as-is, we perpetuate our skew, tiny files, and other problems.
// MAGIC 
// MAGIC **Repartition the dataframes so that each part-file, on disk, is ~100 MB each:**
// MAGIC * We'll call anything between 95 and 115 MBs good.
// MAGIC * There are at least three different strategies for solving this.
// MAGIC   * If you need a temp file, use **tempParquetPath** (declared below)
// MAGIC * You don't want the most accurate method, but the most efficient.
// MAGIC * Write the final parquet file to ** *USERHOME*/tuning/capstone-*YEAR*.parquet ** where
// MAGIC   * ** *USERHOME* ** is your home directory defined the variable **userhome**
// MAGIC   * ** *YEAR* ** is the year correspending to the set of transactions being processed.

// COMMAND ----------

// TODO

// Just some place to write a temp file to
val tempDir = s"$userhome/tuning/capstone"
val tempParquetPath = "%s/temp.parquet".format(tempDir)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #5
// MAGIC 
// MAGIC To validate your work, run the following cell.
// MAGIC 
// MAGIC **Note:** This is just here to help keep you on track.

// COMMAND ----------

def validatePartitions(year:Int):Unit = {
  val path = s"$userhome/tuning/capstone-$year.parquet"
  val sizes = dbutils.fs.ls(path).map(_.size / 1000 / 1000).filter(_ > 0)
  for (i <- 0 until sizes.size) {
    val size = sizes(i)
    validateYourAnswer(s"05.$year.#$i) Between 95 & 115", 646192812, size >= 95 && size <= 115)
    println(s"05.$year.#$i) Actual size: $size\n")
  }
}

// COMMAND ----------

clearYourResults()
validatePartitions(2011)

// COMMAND ----------

clearYourResults()
validatePartitions(2012)

// COMMAND ----------

clearYourResults()
validatePartitions(2013)

// COMMAND ----------

clearYourResults()
validatePartitions(2014)

// COMMAND ----------

clearYourResults()
validatePartitions(2015)

// COMMAND ----------

clearYourResults()
validatePartitions(2016)

// COMMAND ----------

clearYourResults()
validatePartitions(2017)

// COMMAND ----------

clearYourResults()
validatePartitions(2018)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #6A & #6B
// MAGIC 
// MAGIC At this point our on-disk issues with each year of data should be fixed.
// MAGIC 
// MAGIC We can efficiently execute queries on one dataset at a time.
// MAGIC 
// MAGIC We now need to prepare to execute queries on the entire dataset.
// MAGIC 
// MAGIC **Create a single `DataFrame` that consists of all 8 years (2011 to 2018):**
// MAGIC * Assign the final `DataFrame` to **dfAll**
// MAGIC * The solution should 
// MAGIC   * Keep to the previous guidelines, namely ~100 MB per part file
// MAGIC   * Lend itself to easily adding new datasets (e.g. 2019, 2020, etc)

// COMMAND ----------

// TODO

val dfAll = FILL_IN

// COMMAND ----------

// MAGIC %md
// MAGIC ### Goal for Challenge #6A
// MAGIC * Execute a count of all records in under 20 seconds.
// MAGIC * One solution in particular is quicker to implement and still yields decent runtimes.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Challenge Goal for 6B
// MAGIC * Execute a count of all records in under 2 seconds.
// MAGIC * The hardest of all the challenges, this solution takes a bit more work to setup.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #6A & #6B
// MAGIC 
// MAGIC To validate your work, run the following cell.
// MAGIC 
// MAGIC **Note:** Benchmarking can be finicky. 
// MAGIC   * Occasionally (due to various uncontrollable circumstances) these queries can take up to 4x the average. 
// MAGIC   * Run the query 3-4 times to establish an average.

// COMMAND ----------

val results = tracker.track(() => {
  dfAll.count
})

println(f"Duration:      ${results.duration/1000.0}%,.1f seconds")
println(f"Total Records: ${results.result}%,d")
println("-"*80)

// COMMAND ----------

clearYourResults()
validateYourAnswer(s"06) Final Record Count", 2074954664, results.result)
validateYourAnswer(s"06.A) Less than 20 seconds", 646192812, results.duration < 20*1000)
validateYourAnswer(s"06.B) Less than 2 seconds", 646192812, results.duration < 2*1000)
summarizeYourResults()


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>