// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Extracting More Information
// MAGIC 
// MAGIC **Dataset:**
// MAGIC * This is synthetic data generated specifically for these exercises
// MAGIC * Each year's data is roughly the same with some variation for market growth
// MAGIC * We are looking at retail purchases from the top N retailers
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Learn how to diagnose a new performance problem

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %run "./Includes/Initialize-Labs"

// COMMAND ----------

// MAGIC %run "./Includes/Utility-Methods"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Spoiler Alert!
// MAGIC 
// MAGIC We are going to induce a bug in Databricks.
// MAGIC 
// MAGIC Knowing this **might** help with the diagnostics, but not too much.
// MAGIC 
// MAGIC But the bug will only appear in Databricks Runtime Version 4.0

// COMMAND ----------

clearYourResults()
validateYourAnswer("00) Expected Databricks Runtime Version 4.0", 2004184307, dbrVersion)
summarizeYourResults()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC If the assertion above fails, we need to change our cluster's runtime version:
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Databricks has deprecated DBR 4.0 so it requires a small hack to enable it.
// MAGIC 
// MAGIC Update your cluster to use DBR 4.0
// MAGIC * From **THIS notebook**...
// MAGIC   * Open the JavaScript console. 
// MAGIC     * Chrome / Windows: **[Ctrl]** + **[Shift]** + **[J]**
// MAGIC     * Chrome / Mac: **[Cmd]** + **[Option]** + **[J]**
// MAGIC   * In the console, enter the command <span style="color:blue; font-weight:bold">window.prefs.set("enableCustomSparkVersions", true)</span> and press **[Enter]**
// MAGIC * Edit your cluster.
// MAGIC   * After the field **Databricks Runtime Version** there should be a new field, **Custom Spark Version**
// MAGIC   * Set **Custom Spark Version** to <span style="color:blue; font-weight:bold">4.0.x-scala2.11</span>
// MAGIC   * Save the change and restart your cluster
// MAGIC * Rerun **ALL** the cells above including **Classroom-Setup**, **Initialize-Labs** and **Utility-Methods**

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Introduction
// MAGIC 
// MAGIC We have been looking at some generic statistics about our finacial transactions.
// MAGIC 
// MAGIC Embedded in the description of every transaction is a wealth of information.
// MAGIC 
// MAGIC In this section, we are going to look at parsing out that data for futher analysis.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Data for 2014 & 2017

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

// Specify the schema just to save a few seconds on the creation of the DataFrame
val schema = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer"

var path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
val trx2017DF = spark.read.schema(schema).parquet(path2017)

// COMMAND ----------

// MAGIC %md
// MAGIC To expedite our work, once again, we will load the "fast" version of the 2017 dataset.
// MAGIC 
// MAGIC This way we don't have to sit idle while processing 100% of the data after each iteration over our code.
// MAGIC 
// MAGIC When we are all done, we can always test against the full dataset.

// COMMAND ----------

val fastPath17 = "/mnt/training/global-sales/solutions/2017-fast.parquet"
val fast2017DF = spark.read.parquet(fastPath17)

// COMMAND ----------

// MAGIC %md
// MAGIC If we look closely, we can see that we have a `trx_id` a `retailer_id` and a `city_id`.
// MAGIC 
// MAGIC The last two provide additional information about the retailer and the location of the transaction in the corresponding lookup tables.
// MAGIC 
// MAGIC For the sake of this exercise we are going to ignore these details and assume that they need to be parsed out, where available.

// COMMAND ----------

display(fast2017DF)

// COMMAND ----------

// MAGIC %md
// MAGIC Our end goal is to identify the retailer of each transaction.
// MAGIC 
// MAGIC One solution is to peel off data from the right.
// MAGIC 
// MAGIC With each iteration, we can extract that data into new columns for future analysis.
// MAGIC 
// MAGIC There are better solutions, but they are usually a result of refactoring code produced by a process such as this.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Extracting the posted-on date
// MAGIC 
// MAGIC If we look at the data closely, we can see when the transaction was posted (mm-yy) as being the only value that is always the last value.
// MAGIC 
// MAGIC That means we can use the following regular expression to parse it out:

// COMMAND ----------

val postedOnPattern = """(.*\S)(\s*)(\d\d-\d\d)"""

val postedOnTempDF = fast2017DF.withColumn("postedOn", when(regexp_extract($"description", postedOnPattern, 3) =!= "", regexp_extract($"description", postedOnPattern, 3)).otherwise(lit(null)))

// Force an evaluation 
// of the entire dataset
postedOnTempDF.count()

display(postedOnTempDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Then we can use that same regular expression to remove that value from the description.

// COMMAND ----------

val condition = when($"postedOn".isNotNull, regexp_extract($"description", postedOnPattern, 1)).
                otherwise($"description")

val postedOnDF = postedOnTempDF.withColumn("description", condition)

// Force an evaluation 
// of the entire dataset
postedOnDF.count()

display(postedOnDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
// MAGIC 
// MAGIC ### Once Is Chance, Twice is Coincidence, and the Third Time Is A Pattern
// MAGIC 
// MAGIC Normally we would do this only after the pattern emerges.
// MAGIC 
// MAGIC But take my word for it, there is value in wrapping those transformations into utility functions.
// MAGIC 
// MAGIC **Create the first function to extract data given a regular expression:**
// MAGIC * Declare a function named **extractValue**
// MAGIC * The function should return **DataFrame**
// MAGIC * The function should have the following four parameters:
// MAGIC   0. **df**: **DataFrame**
// MAGIC   0. **columnName**: **String**
// MAGIC   0. **pattern**: **String**
// MAGIC   0. **group**: **Int**
// MAGIC * Using the first of our two transformations (the one assigned to **postedOnTempDF**);
// MAGIC   * Return its resulting DataFrame
// MAGIC   * Update the transformation to make use of the four parameters

// COMMAND ----------

// ANSWER

def extractValue(df:DataFrame, columnName:String, pattern:String, group:Int):DataFrame = {
  return df.withColumn(columnName, when(regexp_extract($"description", pattern, group) =!= "", regexp_extract($"description", pattern, group)).otherwise(lit(null)))
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1

// COMMAND ----------

clearYourResults()

val postedOnTempDF = extractValue(df=fast2017DF, columnName="posted_on", pattern=postedOnPattern, group=3)

validateYourSchema("01.A) postedOnTempDF", postedOnTempDF, "transacted_at", "timestamp")
validateYourSchema("01.B) postedOnTempDF", postedOnTempDF, "trx_id", "integer")
validateYourSchema("01.C) postedOnTempDF", postedOnTempDF, "retailer_id", "integer")
validateYourSchema("01.D) postedOnTempDF", postedOnTempDF, "description", "string")
validateYourSchema("01.E) postedOnTempDF", postedOnTempDF, "amount", "decimal")
validateYourSchema("01.F) postedOnTempDF", postedOnTempDF, "city_id", "integer")
validateYourSchema("01.G) postedOnTempDF", postedOnTempDF, "posted_on", "string")

val count = postedOnTempDF.select("posted_on").distinct().count()
validateYourAnswer("01.H) Expected 366 distinct dates", toHash("366"), count)

summarizeYourResults()

// COMMAND ----------

display(postedOnTempDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #2
// MAGIC 
// MAGIC **Create the second function to update the description by removing the value:**
// MAGIC * Declare a function named **updateDescription**
// MAGIC * The function should return **DataFrame**
// MAGIC * The function should have the following three parameters:
// MAGIC   0. **df**: **DataFrame**
// MAGIC   0. **columnName**: **String**
// MAGIC   0. **pattern**: **String**
// MAGIC * Using the first of our two transformations (the one assigned to **postedOnDF**);
// MAGIC   * Return its resulting DataFrame
// MAGIC   * Update the transformation to make use of the three parameters

// COMMAND ----------

// ANSWER

def updateDescription(df:DataFrame, columnName:String, pattern:String):DataFrame = {
  return df.withColumn("description", when(col(columnName).isNotNull, regexp_extract($"description", pattern, 1)).otherwise($"description"))
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #2

// COMMAND ----------

clearYourResults()

val postedOnDF = updateDescription(df=postedOnTempDF, columnName="posted_on", pattern=postedOnPattern)

validateYourSchema("02.A) postedOnTempDF", postedOnTempDF, "transacted_at", "timestamp")
validateYourSchema("02.B) postedOnTempDF", postedOnTempDF, "trx_id", "integer")
validateYourSchema("02.C) postedOnTempDF", postedOnTempDF, "retailer_id", "integer")
validateYourSchema("02.D) postedOnTempDF", postedOnTempDF, "description", "string")
validateYourSchema("02.E) postedOnTempDF", postedOnTempDF, "amount", "decimal")
validateYourSchema("02.F) postedOnTempDF", postedOnTempDF, "city_id", "integer")
validateYourSchema("02.G) postedOnTempDF", postedOnTempDF, "posted_on", "string")

val count = postedOnDF.filter($"description".endsWith("11-25")).count()
validateYourAnswer("02.H) Expected 0 descriptions ending in 11-25", toHash("0"), count)

summarizeYourResults()

// COMMAND ----------

display(postedOnDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) More Extractions

// COMMAND ----------

// MAGIC %md
// MAGIC ### Extracting the location
// MAGIC 
// MAGIC Working right-to-left, the next value should be the location. 

// COMMAND ----------

val locationPattern = """(.*)  ([A-Z][a-z].*)"""

var locationTempDF = extractValue(postedOnDF, "location", locationPattern, 2)
var locationDF = updateDescription(locationTempDF, "location", locationPattern)

// Force an evaluation 
// of the entire dataset
locationDF.count()

display(locationDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Extracting the transaction ID and type
// MAGIC 
// MAGIC This one is just a little bit different because there are actually two values to extract:
// MAGIC * **transactionId** - the ID of the transaction
// MAGIC * **transactionType** - the type of transaction (**ppd**, **arc**, **ccd**)
// MAGIC 
// MAGIC But the process is roughly the same:

// COMMAND ----------

val trxPattern =      """(.*\S)(.*\W\W)([a-z][a-z][a-z])\sid:(\W[0-9]+)"""

var trxIdTempADF = extractValue(locationDF, "transactionId", trxPattern, 4)
var trxIdTempBDF = extractValue(trxIdTempADF, "transactionType", trxPattern, 3)
var trxIdDF = updateDescription(trxIdTempBDF, "transactionId", trxPattern)

// Force an evaluation 
// of the entire dataset
trxIdDF.count()

display(trxIdDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Extracting the retailer's name
// MAGIC 
// MAGIC All that is left in the description column is the retailer's name.
// MAGIC 
// MAGIC If the column is "unkn" set it to null otherwise trim any whitespace.
// MAGIC 
// MAGIC Rename the column to "retailer"

// COMMAND ----------

val cleanDF = trxIdDF
  .withColumn("retailer", when(trim($"description") === "unkn", lit(null)).otherwise(trim($"description")))
  .drop("description")


// Force an evaluation 
// of the entire dataset
cleanDF.count()

display(cleanDF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #3
// MAGIC 
// MAGIC The last task is to categorize each transaction based on the retailer.
// MAGIC 
// MAGIC For example, **Kroger** is a **Grocery**, **Rite-Aid** is a **Drug Store** and **McDonald's** is a **Restaurant**.
// MAGIC 
// MAGIC **Create a utility method to define a category:**
// MAGIC * Declare a method named **categorize**
// MAGIC * The method should have three parameters:
// MAGIC   * **df**:**DataFrame**
// MAGIC   * **category**:**String**
// MAGIC   * **retailers**:**Seq[String]**
// MAGIC * The method should return the new **DataFrame**
// MAGIC   * The body of the method should consist of one transformation **DataFrame.withColumn(..)**
// MAGIC   * The transformation should consist of a single **when(..).otherwise(..)** column
// MAGIC 
// MAGIC **Categorize each transaction:**
// MAGIC * Use the categories specified below
// MAGIC * The first condition was written for you where **Amazon.com** and **Signet Jewelers** are classified as **Miscellaneous**
// MAGIC * Categorization of the retailers is an iterative process:
// MAGIC   * Run the cell below
// MAGIC   * Use the results to determine which retailers still need to be classified
// MAGIC   * Add new transformations as needed.
// MAGIC   * Repeat
// MAGIC   
// MAGIC **Bonus:** Refactor to iterate over a map index by key and a list of retailers as the key's value

// COMMAND ----------

// ANSWER

def categorize(df:DataFrame, category:String, retailers:Seq[String]): DataFrame = {
  df.withColumn("category", when($"retailer".isin(retailers:_ *), lit(category)).otherwise($"category"))
}

var finalDF = cleanDF.withColumn("category", when($"retailer".isNull, lit("Miscellaneous")).otherwise(lit(null)))
finalDF = categorize(finalDF, "Miscellaneous",     Seq("Amazon.com", "Signet Jewelers"))

display(finalDF)

finalDF = categorize(finalDF, "Drug Store",        Seq("CVS", "Walgreen","Rite Aid", "Alimentation Couche-Tard"))
finalDF = categorize(finalDF, "Convenience Store", Seq("7-Eleven"))
finalDF = categorize(finalDF, "Dining Out",        Seq("YUM! Brands", "McDonald's", "Wendy's", "Subway", "BJ's", "DineEquity","Starbucks"))
finalDF = categorize(finalDF, "Entertainment",     Seq("iTunes", "GameStop", "Dick's Sporting Goods","Michael's"))
finalDF = categorize(finalDF, "Grocery",           Seq("Safeway", "Albertsons", "Trader Joe's", "Costco", "Meijer", "Kroger", "Ahold","Publix Super Markets"))
finalDF = categorize(finalDF, "House Hold",        Seq("Wal-Mart", "Sears", "Target", "Ikea","AVB Brandsource"))
finalDF = categorize(finalDF, "Home Improvement",  Seq("Home Depot", "Ace Hardware", "Menard"))
finalDF = categorize(finalDF, "Clothing",          Seq("Macy's", "TJ Max", "Kohl's", "J.C. Penney"))

// Force an evaluation 
// of the entire dataset
finalDF.count()

display(
  finalDF.filter($"category".isNull)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) View the results of your aggregation

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 16)

val results = finalDF
  .groupBy("category").sum("amount")
  .withColumnRenamed("sum(amount)", "amount")
  .collect

// COMMAND ----------

var jsonData = ""
for (row <- results) {
  jsonData += s"          ['${row.getString(0)}', ${row.getDecimal(1)}],\n"
}

// Remove the last comma from the string
jsonData = jsonData.substring(0, jsonData.size-2)

println(jsonData)
println("-"*80)

// COMMAND ----------

val categoryCol = "Category"
val valueCol = "Amount"

var html = s"""<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['corechart']});
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {

        var data = google.visualization.arrayToDataTable([
          ['%s', 'valueCol'],
            %s
          ]);

        var options = {
          title: 'Spending By Payee'
        };

        var chart = new google.visualization.PieChart(document.getElementById('piechart'));

        chart.draw(data, options);
      }
    </script>
  </head>
  <body style="padding:0">
    <div id="piechart" style="border:1px solid black; paddding:0; margin:0; width: 800px; height: 400px;"></div>
  </body>
</html>""".format(categoryCol, jsonData)

displayHTML(html)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/> Review Challenge #3</h2>
// MAGIC 
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #4
// MAGIC 
// MAGIC By now you should have ran into a rather significant problem.
// MAGIC 
// MAGIC As you step through the code, adding one transformation at a time, you can see that at some point performance takes a big dive:
// MAGIC * ~5 seconds
// MAGIC * ~8 seconds
// MAGIC * ~10 seconds
// MAGIC * ~12 seconds
// MAGIC * ~17 seconds
// MAGIC * ~25 seconds
// MAGIC * ~40 seconds
// MAGIC * ~1.25 minutes
// MAGIC * It can very rapidly get up to 10 minutes.
// MAGIC 
// MAGIC **WARNING**: Before you get too much further make sure you open, and keep open, the Spark UI.
// MAGIC   * If you happen to crash the cluster, getting back to the Spark UI can be a bit challenging.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC     <link rel="stylesheet" type="text/css" href="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/static/assets/spark-ilt/labs.css">
// MAGIC   </head>
// MAGIC   <body>
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #1</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>What is the problem?</h2>
// MAGIC 
// MAGIC       <p><button style="width:20em" onclick="block('wa-1', 'ca', 'wa-2')">Degraded Performance</button></p>
// MAGIC       <p><button style="width:20em" onclick="block('ca', 'wa-1', 'wa-2')">Application Crashed</button></p>
// MAGIC       <p><button style="width:20em" onclick="block('wa-2', 'ca', 'wa-1')">Wrong Result</button></p>
// MAGIC 
// MAGIC       <div id="ca" style="display:none">
// MAGIC         <p>Good - You can force this app to crash by adding more `withColumn()` transformations.</p>
// MAGIC         <p class="next-step">Go to the next step</p>
// MAGIC       </div>
// MAGIC 
// MAGIC       <p id="wa-2" style="display:none">Are you sure?</p>
// MAGIC 
// MAGIC       <div id="wa-1" style="display:none">
// MAGIC         <h2>Q1-A: Is the performance degradation progressive?</h2>
// MAGIC         That is if you add more records or more transformations does the performance degrade in an unexpected manner?
// MAGIC         <li>Note #1: Adding more records will take longer, that's a given, but does it increase in a non-linear manner?</li>
// MAGIC         <li>Note #2: Adding more wide transformations will also increase execution time, however narrow transformations generally should have little to no effect on execution time.</li>
// MAGIC 
// MAGIC         <p>
// MAGIC           <button style="width:3em" onclick="inline('push-it', 'not-prog')">Yes</button>
// MAGIC           &nbsp;
// MAGIC           <span id="push-it" style="display:none">If the degradation is progressive, you should be able to push it to fail - this can help to further diagnose the problem.</span>
// MAGIC         </p>
// MAGIC 
// MAGIC         <div style="margin-top:1em">
// MAGIC           <button style="width:3em" onclick="inline('not-prog','push-it')">No</button>
// MAGIC           &nbsp;
// MAGIC           <span id="not-prog" style="display:none">Are you sure?</span>
// MAGIC         </div>
// MAGIC 
// MAGIC       </div>
// MAGIC     </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC     <link rel="stylesheet" type="text/css" href="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/static/assets/spark-ilt/labs.css">
// MAGIC   </head>
// MAGIC   <body>
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #2</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>Where did the application crash?</h2>
// MAGIC 
// MAGIC       <div style="margin-top:1em"><button style="width:15em" onclick="block('ca', 'wa')">In the Driver</button></div>
// MAGIC       <div style="margin-top:1em"><button style="width:15em" onclick="block('wa', 'ca')">In an Executor</button></div>
// MAGIC 
// MAGIC       <div id="ca" style="display:none; margin-top:1em">
// MAGIC         There are a number of different clues:
// MAGIC         <li>In the driver's log file you should see the offending exception (hint: search the three log files for the word **Exception** or **Error**)</li>
// MAGIC         <li>Just below the cell that crashed, Databricks shows the message: **The spark driver has stopped unexpectedly and is restarting. Your notebook will be automatically reattached.**</li>
// MAGIC         <p class="next-step">Go to the next step</p>
// MAGIC       </div>
// MAGIC       <div id="wa" style="display:none; margin-top:1em">What evidence do you have that this crash was in an executor?</div>
// MAGIC 
// MAGIC     </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC   </head>
// MAGIC   <body>
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #3</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>How did the application crash?</h2>
// MAGIC 
// MAGIC       <div style="margin-top:1em"><button style="width:20em; margin-right: 1em" onclick="block('wa-1', 'ca-1', 'ca-2', 'wa-3', 'wa-4')">A Programming Bug</button> An example of this type would be a **NullPointerError** or **ArithmeticException**</div>
// MAGIC       <div style="margin-top:1em"><button style="width:20em; margin-right: 1em" onclick="block('ca-2', 'ca-1', 'wa-1', 'wa-3', 'wa-4')">OOM: Java heap space</button>This one occurs when too many objects have been allocated.           <div style="margin-top:1em"><button style="width:20em; margin-right: 1em" onclick="block('ca-1', 'wa-1', 'ca-2', 'wa-3', 'wa-4')">OOM: GC overhead limit exceeded</button> A specific variant of OOM in which the JVM is terminated for spending too much time in garbage collection.</div>
// MAGIC   </div>
// MAGIC       <div style="margin-top:1em"><button style="width:20em; margin-right: 1em" onclick="block('wa-3', 'ca-1', 'wa-1', 'ca-2', 'wa-4')">OOM: Other Variant</button>This would include variants like **PermGen space**, **Metaspace**, etc.</div>
// MAGIC       <div style="margin-top:1em"><button style="width:20em; margin-right: 1em" onclick="block('wa-4', 'ca-1', 'wa-1', 'ca-2', 'wa-3')">VM Crashed</button>Fairly rare, but in some occasions your cloud provider's VM may have been terminated or crashed.</div>
// MAGIC 
// MAGIC       <div id="ca-1" style="display:none; margin-top:1em">
// MAGIC         <div>If you found in the driver's log file the error **java.lang.OutOfMemoryError: GC overhead limit exceeded**.</div>
// MAGIC         <div style="margin-top:1em">What this means is that:</div>
// MAGIC         <li>98% of the JVM's time is being spent in GC.</li>
// MAGIC         <li>Less than 2% of the heap is being relcaimed during a full GC cyle.</li>
// MAGIC         <li>Both conditions are true for 5 consecutive full GC cyles.</li>
// MAGIC         <div style="margin-top:1em; font-weight:bold; color:green">Go to the next step</div>
// MAGIC       </div>
// MAGIC 
// MAGIC       <div id="ca-2" style="display:none; margin-top:1em">
// MAGIC         <div>If you found in the driver's log file the error **java.lang.OutOfMemoryError: Java heap space**.</div>
// MAGIC         <div style="margin-top:1em">What this means is that the driver has used all of its heap space.</div>
// MAGIC         <div style="margin-top:1em; font-weight:bold; color:green">Go to the next step</div>
// MAGIC       </div>
// MAGIC 
// MAGIC       <div id="wa-1" style="display:none; margin-top:1em">It's not a programming bug - did you find the exception in the driver's log?</div>
// MAGIC       <div id="wa-3" style="display:none; margin-top:1em">It is an OOM, but not one of these - did you find the exception in the driver's log?</div>
// MAGIC       <div id="wa-4" style="display:none; margin-top:1em">The underlying VM didn't crash, just the driver - did you find the exception in the driver's log?</div>
// MAGIC 
// MAGIC     </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC   </head>
// MAGIC   <body>
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #4</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>Why are we spending so much time in GC?</h2>
// MAGIC 
// MAGIC       <div style="margin-top:1em">In all honesty, this is where traditional troubleshooting starts to fail us.</div>
// MAGIC 
// MAGIC       <div style="margin-top:1em">
// MAGIC         <div>There appears to be no reason for this app to be crashing:</div>
// MAGIC         <li>Only a single long value is being returned to the driver</li>
// MAGIC         <li>The transformations we are executing are fairly straightforward</li>
// MAGIC         <li>There really isn't that many transformations to begin with</li>
// MAGIC         <li>The flavor of **OOM** does give us some clue</li>
// MAGIC       </div>
// MAGIC 
// MAGIC       <div style="margin-top:1em">Why are we spending so much time in GC or maxing out the Heap?</div>
// MAGIC 
// MAGIC       <div style="margin-top:1em">We can pour through the Jobs, Stages & Tasks but this bug is in the Driver...</div>
// MAGIC 
// MAGIC       <div style="margin-top:1em">
// MAGIC         <div>One thing we haven't really looked at is the Physical Plan.</div>
// MAGIC         <li>Go to the Spark UI</li>
// MAGIC         <li>Select the SQL tab</li>
// MAGIC         <li>Select your query (cross reference the job IDs on the right)</li>
// MAGIC         <li>Click on the **Details** link at the bottom of the screen to open the Physical Plan</li>
// MAGIC       </div>
// MAGIC 
// MAGIC       <div style="margin-top:1em; font-weight:bold; color:green">Go to the next step</div>
// MAGIC 
// MAGIC     </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC   </head>
// MAGIC   <body>
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #5</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>A REALLY complex Plan</h2>
// MAGIC 
// MAGIC       <div style="margin-top:1em">If you look at the Logical Plan, you can see that it is really straightforward.</div>
// MAGIC       <div style="padding:1em; font-size:8pt; white-space:pre; border:1px groove black; width:100%; height:100%; ">== Parsed Logical Plan ==
// MAGIC Project [category#688, sum(amount)#724 AS amount#727]
// MAGIC +- AnalysisBarrier
// MAGIC       +- Aggregate [category#688], [category#688, sum(amount#53) AS sum(amount)#724]
// MAGIC          +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, CASE WHEN isnull(category#677) THEN misc ELSE category#677 END AS category#688]
// MAGIC             +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, CASE WHEN description#425 IN (robbins bros) THEN misc ELSE category#666 END AS category#677]
// MAGIC                +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, CASE WHEN description#425 IN (raising cane's,sonic drive in,papa john's,whataburger,panda express,burger king,dairy queen,denny's,taco bell,rubio's,jack in the box,chick-fil-a,legal sea foods,cracker barrel) THEN dining out ELSE category#655 END AS category#666]
// MAGIC                   +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, CASE WHEN description#425 IN (ross store,torrid,tj maxx) THEN clothing ELSE category#644 END AS category#655]
// MAGIC                      +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, CASE WHEN description#425 IN (jo-ann store,amc metreon 16,amc southcenter) THEN entertainment ELSE category#633 END AS category#644]
// MAGIC                         +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, CASE WHEN description#425 IN (autozone,jiffy lube) THEN automotive ELSE category#622 END AS category#633]
// MAGIC                            +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, CASE WHEN description#425 IN (raley's,winco foods,safeway,tom thumb,vons) THEN grocery ELSE category#611 END AS category#622]
// MAGIC                               +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, CASE WHEN description#425 IN (petro wheeler ridge,arco ampm,kroger fuel ctr,usa petro) THEN gas ELSE category#600 END AS category#611]
// MAGIC                                  +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, CASE WHEN description#425 IN (check) THEN unknown ELSE category#589 END AS category#600]
// MAGIC                                     +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, CASE WHEN description#425 IN (wm supercenter,homegoods,wal-mart,lowes,petsmart,costco whse,cvs/pharmacy) THEN home goods ELSE cast(category#578 as string) END AS category#589]
// MAGIC                                        +- Project [transactionDate#51, description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357, null AS category#578]
// MAGIC                                           +- Project [transactionDate#51, CASE WHEN (cast(checkNumber#302 as int) > 0) THEN concat(check #, checkNumber#302) ELSE description#367 END AS description#425, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357]
// MAGIC                                              +- Project [transactionDate#51, CASE WHEN isnotnull(storeNumber#357) THEN regexp_extract(description#250, (.*)( #)([0-9]+), 1) ELSE description#250 END AS description#367, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, storeNumber#357]
// MAGIC                                                 +- Project [transactionDate#51, description#250, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, checkNumber#302, CASE WHEN NOT (regexp_extract(description#250, (.*)( #)([0-9]+), 3) = ) THEN regexp_extract(description#250, (.*)( #)([0-9]+), 3) ELSE cast(0 as string) END AS storeNumber#357]
// MAGIC                                                    +- Project [transactionDate#51, description#250, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242, CASE WHEN NOT (regexp_extract(description#250, (check #)([0-9]+), 2) = ) THEN regexp_extract(description#250, (check #)([0-9]+), 2) ELSE cast(0 as string) END AS checkNumber#302]
// MAGIC                                                       +- Project [transactionDate#51, CASE WHEN isnotnull(transactionId#235) THEN regexp_extract(description#189, (.*\S)(.*\W\W)([a-z][a-z][a-z])\sid:(\W[0-9]+), 1) ELSE description#189 END AS description#250, amount#53, postedOn#135, location#183, transactionId#235, transactionType#242]
// MAGIC                                                          +- Project [transactionDate#51, description#189, amount#53, postedOn#135, location#183, transactionId#235, CASE WHEN NOT (regexp_extract(description#189, (.*\S)(.*\W\W)([a-z][a-z][a-z])\sid:(\W[0-9]+), 3) = ) THEN regexp_extract(description#189, (.*\S)(.*\W\W)([a-z][a-z][a-z])\sid:(\W[0-9]+), 3) ELSE cast(null as string) END AS transactionType#242]
// MAGIC                                                             +- Project [transactionDate#51, description#189, amount#53, postedOn#135, location#183, CASE WHEN NOT (regexp_extract(description#189, (.*\S)(.*\W\W)([a-z][a-z][a-z])\sid:(\W[0-9]+), 4) = ) THEN regexp_extract(description#189, (.*\S)(.*\W\W)([a-z][a-z][a-z])\sid:(\W[0-9]+), 4) ELSE cast(null as string) END AS transactionId#235]
// MAGIC                                                                +- Project [transactionDate#51, CASE WHEN isnotnull(location#183) THEN regexp_extract(description#140, (.*\S)(.*\W\W)(\w*\s*\w*,\s[A-Z][A-Z]), 1) ELSE description#140 END AS description#189, amount#53, postedOn#135, location#183]
// MAGIC                                                                   +- Project [transactionDate#51, description#140, amount#53, postedOn#135, CASE WHEN NOT (regexp_extract(description#140, (.*\S)(.*\W\W)(\w*\s*\w*,\s[A-Z][A-Z]), 3) = ) THEN regexp_extract(description#140, (.*\S)(.*\W\W)(\w*\s*\w*,\s[A-Z][A-Z]), 3) ELSE cast(null as string) END AS location#183]
// MAGIC                                                                      +- Project [transactionDate#51, CASE WHEN isnotnull(postedOn#135) THEN regexp_extract(description#52, (.*\S)(\s*)(\d\d/\d\d), 1) ELSE description#52 END AS description#140, amount#53, postedOn#135]
// MAGIC                                                                         +- Project [transactionDate#51, description#52, amount#53, CASE WHEN NOT (regexp_extract(description#52, (.*\S)(\s*)(\d\d/\d\d), 3) = ) THEN regexp_extract(description#52, (.*\S)(\s*)(\d\d/\d\d), 3) ELSE cast(null as string) END AS postedOn#135]
// MAGIC                                                                            +- Repartition 8, true
// MAGIC                                                                               +- Relation[transactionDate#51,description#52,amount#53] csv</div>
// MAGIC 
// MAGIC         <div style="margin-top:1em">However, if you look at the <a href="https://files.training.databricks.com/static/assets/spark-ilt/tuning/lab-FDWER-physical-plan.txt" target="_blank">Physical Plan</a> (and yes it's too large to show in this notebook) you can see that it's REALLY complex</div>
// MAGIC 
// MAGIC         <div style="margin-top:1em; font-weight:bold; color:green">Go to the next step</div>
// MAGIC 
// MAGIC       </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC   </head>
// MAGIC   <body>
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #6</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>What options do I have?</h2>
// MAGIC 
// MAGIC       <div style="margin-top:1em">Off the top of our head, two options should come to mind (and there is actually a third):</div>
// MAGIC 
// MAGIC       <div style="margin-top:1em"><button style="width:15em" onclick="block('wa-1', 'ca-1', 'wa-2')">Provision a Bigger Driver</button> In short, give the driver more memory.</div>
// MAGIC 
// MAGIC       <div style="margin-top:1em"><button style="width:15em" onclick="block('wa-2', 'ca-1', 'wa-1')">Refactor The Code</button> Refactor the code so that we produce a much simpler physical plan.</div>
// MAGIC 
// MAGIC       <div style="margin-top:1em"><button style="width:15em" onclick="block('ca-1', 'wa-1', 'wa-2')">Mystery Option #3</button></div>
// MAGIC       
// MAGIC       <div id="wa-1" style="display:none">
// MAGIC         <div style="margin-top:1em">Provision a driver with more memory - and this will work.</div>
// MAGIC         <div style="margin-top:1em">However, as we add more and more transformations, will it still hold up?</div>
// MAGIC         <div style="margin-top:1em">Especially when we are seeing a tripling of execution time with every new transformation?</div>
// MAGIC       </div>
// MAGIC       
// MAGIC       <div id="wa-2" style="display:none">
// MAGIC         <div style="margin-top:1em">But again, as you increase the number of transformations, you will only get so far before it starts to fail again.</div>
// MAGIC       </div>
// MAGIC       
// MAGIC       <div id="ca-1" style="display:none">
// MAGIC         <div style="margin-top:1em">By all accounts our code above should work!</div>
// MAGIC         <div style="margin-top:1em">It doesn't make sense that a 28 line Logical Plan explodes into a Physical Plan that is over a 1/2 million characters long!</div>
// MAGIC         <div style="margin-top:1em">And while I might be able to compensate for this in the short term, I shouldn't have to.</div>
// MAGIC         <div style="margin-top:1em; font-weight:bold">There is something wrong with this version of Spark!</div>
// MAGIC         <div style="margin-top:1em; font-weight:bold; color:green">Go to the next step</div>
// MAGIC       </div>
// MAGIC       
// MAGIC     </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC   </head>
// MAGIC   <body>
// MAGIC 
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #7</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>Is there a bug in Spark?</h2>
// MAGIC 
// MAGIC       <div style="margin-top:1em">At this stage it's a fair conclusion.</div>
// MAGIC 
// MAGIC       <div style="margin-top:1em">One that should be investigated <strong>BEFORE</strong> investing in a significant refactor.</div>
// MAGIC 
// MAGIC       <div style="margin-top:1em">**ESPECIALLY** when one considers how easy it is to test this theory.</div>
// MAGIC 
// MAGIC       <div style="margin-top:1em">
// MAGIC         <li>Go to the top of this notebook and comment out the line **requireDbrVersion("4.0")**</li>
// MAGIC         <li>Re-create the cluster using **DBR 4.1** or better.</li>
// MAGIC         <li>Re-run the notebook, **Run All** should suffice.</li>
// MAGIC       </div>
// MAGIC 
// MAGIC       <div style="margin-top:1em; font-weight:bold; color:green">Go to the next step</div>
// MAGIC     </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC   </head>
// MAGIC   <body>
// MAGIC 
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #8</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>Confirmed, we have a bug in Spark!</h2>
// MAGIC 
// MAGIC       <div style="margin-top:1em">Congratulations, your code should be running at ~30 seconds.</div>
// MAGIC 
// MAGIC       <div style="margin-top:1em">And while you are at it, take a look at that Physical Plan...</div>
// MAGIC 
// MAGIC     </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #5
// MAGIC 
// MAGIC Our utility method resulted in some sloppy code.
// MAGIC 
// MAGIC More to the point, it led us into chaining transformation after transformation.
// MAGIC 
// MAGIC A better alternative is to use a single transformation with a sequence of **when(..)** calls as in:
// MAGIC   * **when(..).when(..).when(..).otherwise(..)**
// MAGIC   
// MAGIC **Refactor the code:**
// MAGIC * Go to the top of this notebook and **uncomment** out the line **requireDbrVersion("4.0")**
// MAGIC * Re-create the cluster using DBR 4.0 (specifically).
// MAGIC * Start from **cleanDF** and refactor the code to use a single **withColumn(..)** transformation

// COMMAND ----------

// ANSWER

val whenCol = when($"retailer".isNull, lit("Miscellaneous"))
             .when($"retailer".isin("Amazon.com", "Signet Jewelers"), lit("Miscellaneous"))
             .when($"retailer".isin("CVS", "Walgreen","Rite Aid", "Alimentation Couche-Tard"), lit("Drug Store"))
             .when($"retailer".isin("7-Eleven"), lit("Convenience Store"))
             .when($"retailer".isin("YUM! Brands", "McDonald's", "Wendy's", "Subway", "BJ's", "DineEquity","Starbucks"), lit("Dining Out"))
             .when($"retailer".isin("iTunes", "GameStop", "Dick's Sporting Goods","Michael's"), lit("Entertainment"))
             .when($"retailer".isin("Safeway", "Albertsons", "Trader Joe's", "Costco", "Meijer", "Kroger", "Ahold","Publix Super Markets"), lit("Grocery"))
             .when($"retailer".isin("Wal-Mart", "Sears", "Target", "Ikea","AVB Brandsource"), lit("House Hold"))
             .when($"retailer".isin("Home Depot", "Ace Hardware", "Menard"), lit("Home Improvement"))
             .when($"retailer".isin("Macy's", "TJ Max", "Kohl's", "J.C. Penney"), lit("Clothing"))
             .otherwise(lit(null))

var finalDF = cleanDF.withColumn("category", whenCol)

// Eval the full dataset
finalDF.count()

display(
  finalDF.filter($"category".isNull)
)


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>