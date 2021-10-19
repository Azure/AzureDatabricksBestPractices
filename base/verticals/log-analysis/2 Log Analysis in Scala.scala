// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # **Log Analysis in Scala**
// MAGIC This notebook analyzes Apache access logs with Scala.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 1:** Change this parameter to your sample logs directory.
// MAGIC 
// MAGIC In this example, we will use the sample file "/databricks-datasets/sample_logs" that has already been saved on DBFS (Databricks File System).

// COMMAND ----------

val DBFSSampleLogsFolder = "/databricks-datasets/sample_logs"  // path to the log file to be analyzed

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/sample_logs

// COMMAND ----------

// MAGIC %fs head dbfs:/databricks-datasets/sample_logs/part-00025

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 2:** Create a parser for the Apache Access log lines to create case class objects.

// COMMAND ----------

// MAGIC %md
// MAGIC #### **Log Line Format**
// MAGIC 
// MAGIC In order to analyze this data, we need to parse it into a usable format.
// MAGIC 
// MAGIC The log files that we use for this lab are in the [Apache Common Log Format (CLF)](http://httpd.apache.org/docs/1.3/logs.html#common). The log file entries produced in CLF will look something like this:
// MAGIC `127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839`
// MAGIC  
// MAGIC A summary is provided below, describing each part of the log entry.
// MAGIC * `127.0.0.1`
// MAGIC This is the IP address (or host name, if available) of the client (remote host) which made the request to the server.
// MAGIC  
// MAGIC * `-`
// MAGIC The "hyphen" in the output indicates that the requested piece of information (user identity from remote machine) is not available.
// MAGIC  
// MAGIC * `-`
// MAGIC The "hyphen" in the output indicates that the requested piece of information (user identity from local logon) is not available.
// MAGIC  
// MAGIC * `[01/Aug/1995:00:00:01 -0400]`
// MAGIC The time that the server finished processing the request. The format is:
// MAGIC `[day/month/year:hour:minute:second timezone]`
// MAGIC   * day = 2 digits
// MAGIC   * month = 3 letters
// MAGIC   * year = 4 digits
// MAGIC   * hour = 2 digits
// MAGIC   * minute = 2 digits
// MAGIC   * second = 2 digits
// MAGIC   * zone = (\+ | \-) 4 digits
// MAGIC  
// MAGIC * `"GET /images/launch-logo.gif HTTP/1.0"`
// MAGIC This is the first line of the request string from the client. It consists of a three components: the request method (e.g., `GET`, `POST`, etc.), the endpoint (a [Uniform Resource Identifier](http://en.wikipedia.org/wiki/Uniform_resource_identifier)), and the client protocol version.
// MAGIC  
// MAGIC * `200`
// MAGIC This is the status code that the server sends back to the client. This information is very valuable, because it reveals whether the request resulted in a successful response (codes beginning in 2), a redirection (codes beginning in 3), an error caused by the client (codes beginning in 4), or an error in the server (codes beginning in 5). The full list of possible status codes can be found in the HTTP specification ([RFC 2616](https://www.ietf.org/rfc/rfc2616.txt) section 10).
// MAGIC  
// MAGIC * `1839`
// MAGIC The last entry indicates the size of the object returned to the client, not including the response headers. If no content was returned to the client, this value will be "-" (or sometimes 0).
// MAGIC  
// MAGIC Note that log files contain information supplied directly by the client, without escaping. Therefore, it is possible for malicious clients to insert control-characters in the log files, *so care must be taken in dealing with raw logs.*

// COMMAND ----------

// Create a case class for Apache access logs.
case class ApacheAccessLog(ipAddress: String, clientIdentity: String,
                           userId: String, dateTime: String, method: String,
                           endpoint: String, protocol: String,
                           responseCode: Int, contentSize: Long) {

}

// COMMAND ----------

// Define the parsing function.
val Pattern = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

def parseLogLine(log: String): ApacheAccessLog = {
  val res = Pattern.findFirstMatchIn(log)
  if (res.isEmpty) {
    throw new RuntimeException("Cannot parse log line: " + log)
  }
  val m = res.get
  ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
    m.group(5), m.group(6), m.group(7), m.group(8).toInt, m.group(9).toLong)
}


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 3:** Load the log lines into a Spark RDD (Resilient Distributed Dataset).

// COMMAND ----------

val accessLogs = (sc.textFile(DBFSSampleLogsFolder)              
                  // Call the parse_apace_log_line function on each line.
                  .map(parseLogLine))

// COMMAND ----------

accessLogs.count()

// COMMAND ----------

accessLogs.cache()

// COMMAND ----------

accessLogs.count()

// COMMAND ----------

accessLogs.count()

// COMMAND ----------

val accessLogs = (sc.textFile(DBFSSampleLogsFolder)              
                  // Call the parse_apace_log_line function on each line.
                  .map(parseLogLine))
// Caches the objects in memory since they will be queried multiple times.
accessLogs.cache()
// An action must be called on the RDD to actually cache it.
accessLogs.count()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 4:** Calculate statistics on the sizes of the content returned by the GET requests.

// COMMAND ----------

// Create the content sizes rdd.
val contentSizes = (accessLogs
                    .map(log => log.contentSize)
                    .cache()) // Cache this as well since it will be queried many times.

// COMMAND ----------

// Compute the average content size.
val averageContentSize = contentSizes.reduce(_ + _) / contentSizes.count()

// COMMAND ----------

// Compute the minimum content size.
val minContentSize = contentSizes.min()

// COMMAND ----------

// Compute the maximum content size.
val maxContentSize = contentSizes.max()

// COMMAND ----------

println("Content Size Statistics:\n  Avg: %s\n  Min: %s\n  Max: %s".format(averageContentSize, minContentSize, maxContentSize))

// COMMAND ----------

// MAGIC %md ### **Step 5:** Compute statistics on the response codes.

// COMMAND ----------

// First, calculate the response code to count pairs.
val responseCodeToCountPairRdd = (accessLogs
                                  .map(log => (log.responseCode, 1))
                                  .reduceByKey(_ + _))

// COMMAND ----------

// View the responseCodeToCount by calling take on the RDD - which outputs an array of tuples.
// Notice the use of take(100) - just in case bad data may have slipped in and there are too many response codes.
val responseCodeToCountArray = responseCodeToCountPairRdd.take(100)

// COMMAND ----------

// To call display(), the RDD of tuples must be converted to an RDD case classes
case class ResponseCodeToCount(responseCode: Int, count: Int)
// A simple map can convert the tuples to case classes to create a DataFrame.
val responseCodeToCountDataFrame = responseCodeToCountPairRdd.map(t => ResponseCodeToCount(t._1, t._2)).toDF()

// COMMAND ----------

// Now, display can be called on the resulting DataFrame.
//   For this display, a Pie Chart is chosen by selecting that icon.
//   Then, "Plot Options..." was used to make the responseCode the key and count as the value.
display(responseCodeToCountDataFrame)

// COMMAND ----------

// MAGIC %md #### NOTE: Instead of converting to a DataFrame for displaying, Spark SQL could also have been used.
// MAGIC * How to register this table for Spark SQL is shown at the end of the notebook.
// MAGIC * See the **Log Analysis in SQL** Notebook for more details on writing sql queries.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 6:** View a list of IPAddresses that has accessed the server more than N times.

// COMMAND ----------

// Change this number as it makes sense for your data set.
val N = 100

// COMMAND ----------

case class IPAddressCaseClass(ipAddress: String)

val ipAddressesDataFrame = (accessLogs
                            .map(log => (log.ipAddress, 1))
                            .reduceByKey(_ + _)
                            .filter(_._2 > N)
                            .map(t => IPAddressCaseClass(t._1))).toDF()

// COMMAND ----------

display(ipAddressesDataFrame)

// COMMAND ----------

// MAGIC %md #### **Tip:** To see how an rdd is computed, use the **toDebugString()** function.

// COMMAND ----------

println(ipAddressesDataFrame.rdd.toDebugString)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### **Step 7:** Explore Statistics about the endpoints. 

// COMMAND ----------

// Calculate the number of hits for each endpoint.
case class EndpointCounts(endpoint: String, count: Int)

val endpointCountsRdd = (accessLogs
                         .map(log => (log.endpoint, 1))
                         .reduceByKey(_ + _))

val endpointCountsDataFrame = endpointCountsRdd.map(t => EndpointCounts(t._1, t._2)).toDF()

// COMMAND ----------

// Display a plot of the distribution of the number of hits across the endpoints.
//   Select the option to plot over all the results if you have more than 1000 data points.
display(endpointCountsDataFrame)

// COMMAND ----------

// Drill down to the top endpoints.
val topEndpoints = endpointCountsRdd.takeOrdered(10)(new Ordering[(String, Int)] { def compare(a: (String, Int), b: (String, Int)) = b._2 compare a._2 })
val topEndpointsDataFrame = sc.parallelize(topEndpoints).map(t => EndpointCounts(t._1, t._2)).toDF()
display(topEndpointsDataFrame)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### **Extra: Use SQL in a Scala Notebook**
// MAGIC * A DataFrame can registered as a temporary SQL Table.
// MAGIC * Then you can do SQL queries against the data in sql cells in this Scala notebook.
// MAGIC * Results from select statements is SQL are automatically displayed.

// COMMAND ----------

// MAGIC %sql DROP TABLE IF EXISTS SQLApacheAccessLogsTable

// COMMAND ----------

accessLogs.toDF().write.saveAsTable("SQLApacheAccessLogsTable")

// COMMAND ----------

// MAGIC %sql describe SQLApacheAccessLogsTable

// COMMAND ----------

// MAGIC %sql SELECT * FROM SQLApacheAccessLogsTable limit 10;