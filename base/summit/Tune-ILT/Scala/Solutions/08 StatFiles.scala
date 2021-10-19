// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #StatFiles
// MAGIC 
// MAGIC **Dataset:**
// MAGIC * This is synthetic data generated specifically for these exercises
// MAGIC * Each year's data is roughly the same with some variation for market growth
// MAGIC * We are looking at retail purchases from the top N retailers
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Explore a new tool that helps identify problems with datasets on disk.

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

// MAGIC %md
// MAGIC ### Take care of the "basics"

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

spark.catalog.clearCache()

var path2014 = "/mnt/training/global-sales/transactions/2014.parquet"
var path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
var path2018 = "/mnt/training/global-sales/transactions/2018.parquet"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Using StatFiles
// MAGIC 
// MAGIC StatFiles is a collection of utility methods for investigating datasets on disk.
// MAGIC 
// MAGIC Courtesy of Navid Bazzazzadeh, Databricks Technical Architect.
// MAGIC 
// MAGIC To get started, load the functions by including the **StatFiles** notebook.

// COMMAND ----------

// MAGIC %run "./Includes/StatFiles"

// COMMAND ----------

// MAGIC %md
// MAGIC Of interest to us is the method **statFileWithPartitions(..)** which takes three parameters:
// MAGIC * **input_path_para**: the path to our Parquet file
// MAGIC * **partsCol**: the columns by which the data is partitioned
// MAGIC * **input_type**: the type of file (**parquet**, **csv** or **tsv**)
// MAGIC 
// MAGIC Let's take a look at an example using the 2014 data:

// COMMAND ----------

val partitions =  List("year", "month")

val statFile14DF = statFileWithPartitions(path2014, partitions, "parquet")
  .withColumn("series", lit("2014"))
  .withColumn("month", lpad(substring_index($"partition", "_", -1), 2, "0"))
  .orderBy("month")
  .persist()

// Show just the first 5 records
display( statFile14DF.limit(5) )

// COMMAND ----------

// Display and then bar-graph the results
// Keys = "month"
// Values = "num_file"
display(statFile14DF.select("month", "num_file"))

// COMMAND ----------

// Display and then bar-graph the results
// Keys = "month"
// Values = "part_size"
display(statFile14DF.select("month", "part_size"))

// COMMAND ----------

// Display and then bar-graph the results
// Keys = "month"
// Values = "min_size", "avg_size", "max_size"
display(statFile14DF.select("month", "min_size", "avg_size", "max_size"))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
// MAGIC 
// MAGIC * Given the sales volume in November and December, the spikes above are expected.
// MAGIC * We actually need to compare the two datasets (2014 & 2018) to understand their differences.
// MAGIC * One little nuance:
// MAGIC   * Because our keys (the **partition** column) include the year AND month, we cannot get alignment across the x-axis.
// MAGIC   * To solve that, we can simpy parse out the month from the **partition** column
// MAGIC   * Then we can left pad **month** with zeros so as to get a better sort
// MAGIC   * (as seen in the example above)
// MAGIC 
// MAGIC **Graph the 2014 & 2018 data together**
// MAGIC * Run **statFileWithPartitions(..)** 
// MAGIC   * Path: **path2018**
// MAGIC   * Partitions: **year** and **month**
// MAGIC   * Type: **parquet**
// MAGIC * Add to the resulting DF two new columns:
// MAGIC   * The **series** column with the value **2018**
// MAGIC   * The **month** column so that we can align the two datasets using the transformation  
// MAGIC   **.withColumn("month", lpad(substring_index($"partition", "_", -1), 2, "0"))**
// MAGIC * Union the two DataFrames together by name and assign the result to **partitionsDiffDF**
// MAGIC * Render the results as a bar graph
// MAGIC * Compare the various 2014 vs 2018 scenarios, grouping by **series**
// MAGIC   * **month** vs **min_size**
// MAGIC   * **month** vs **avg_size**
// MAGIC   * **month** vs **max_size**
// MAGIC   * **month** vs **num_file**
// MAGIC   * **month** vs **part_size**

// COMMAND ----------

// ANSWER

val partitions =  List("year", "month")

val statFile18DF = statFileWithPartitions(path2018, partitions, "parquet")
  .withColumn("series", lit("2018"))
  .withColumn("month", lpad(substring_index($"partition", "_", -1), 2, "0"))
  .orderBy("month")
  .persist()

val partitionsDiffDF = statFile14DF.unionByName(statFile18DF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Review Challenge #1
// MAGIC 
// MAGIC Run the following cells.
// MAGIC 
// MAGIC Each cell simply presents a different dimension of the **StatFiles** data.

// COMMAND ----------

// Display and then bar-graph the results
// Keys = "month"
// Series groupings = "series"
// Values = one of ["min_size", "avg_size", "max_size", "part_size", "num_file"

display(
  // When graphing, putting the x-axis first and the y-axis second 
  // can make for some inteligent defaults in the Plot Options
  partitionsDiffDF.select("month", "series", "num_file", "min_size", "avg_size", "max_size", "part_size")
)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Not on Databricks?
// MAGIC 
// MAGIC Even if you are not using Databricks, you can always produce an HTML chart with products like <a href="https://developers.google.com/chart/" target="_blank">Google Charts</a>.
// MAGIC 
// MAGIC In this case you create your HTML and write it out, somewhere...

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Did you know that you can write files to **dbfs:/FileStore/...** and then download it from Databricks' web server at **/files/...**<br/>
// MAGIC Databrick's web server won't serve HTML files as expected, but rather initiates a download of them instead.

// COMMAND ----------

def createChart(title:String, yAxis:String, elementId:String, dfA:DataFrame, dfB:DataFrame, column:String):String = {
  
  var html = """
  <script type="text/javascript">
      google.charts.load('current', {'packages':['bar']});
      google.charts.setOnLoadCallback(drawChart);

      function drawChart() {
        var data = google.visualization.arrayToDataTable([
          ['Partition', '2014', '2018'],
"""

  val df = dfA.join(dfB, "month").orderBy("month").persist()
  for (row <- df.select(col("month"), dfA(column), dfB(column)).collect()) {
    html += "          ['%s', %s, %s],\n".format(row(0), row(1), row(2))
  }
  
  html = html.dropRight(2) + """
        ]);

        var options = {
          chart: {
            title: '%s'          
          },
          vAxis: {
            title: '%s'
          }
        };

        var chart = new google.charts.Bar(document.getElementById('%s'));

        chart.draw(data, google.charts.Bar.convertOptions(options));
      }
  </script>
""".format(title.replace("_"," "), yAxis, elementId)
  
  return html
}

// COMMAND ----------

val dimensions = Array( ("min_size", "Minimum Size", "Size in MB"), 
                        ("avg_size", "Average Size", "Size in MB"), 
                        ("max_size", "Maximum Size", "Size in MB"), 
                        ("part_size", "Partition Size", "Size in MB"),
                        ("num_file", "Number of Files", "Count") )

var html = """
<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
"""

for (column <- dimensions) {
  html += createChart("By %s".format(column._2), column._3, "graph-%s".format(column._1), statFile14DF, statFile18DF, column._1)
}

html += """
</head>
  <body>
    <h1>StatFiles</h1>
"""

for (column <- dimensions) {
  html += """<div id="graph-%s" style="width: 800px; height: 500px;"></div>""".format(column._1)
}

html += """
</body>
</html>
"""

val htmlDir = "dbfs:/FileStore/%s".format(username)
dbutils.fs.mkdirs(htmlDir)

val htmlFile = "%s/stat-files.html".format(htmlDir)
dbutils.fs.put(htmlFile, html, true)

val htmlURL = "/files/%s/stat-files.html".format(username)
displayHTML("""<a href="%s">Download Report</a>""".format(htmlURL))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The browser may refuse to display the following code because it is deemed to<br/>
// MAGIC be "unsafe". You may need to make an exception for this domain so that it can be rendered

// COMMAND ----------

displayHTML(html)


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>