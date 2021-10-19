# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # **Log Analysis in Python**
# MAGIC This notebook analyzes Apache access logs with Python.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 1:** Change this parameter to your sample logs directory.
# MAGIC 
# MAGIC In this example, we will use the sample file "/databricks-datasets/sample_logs" that has already been saved on DBFS (Databricks File System).

# COMMAND ----------

DBFS_SAMPLE_LOGS_FOLDER = "/databricks-datasets/sample_logs"  # path to the log file to be analyzed

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/sample_logs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 2:** Create a parser for the Apache Access log lines to create Row objects.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Log Line Format**
# MAGIC 
# MAGIC In order to analyze this data, we need to parse it into a usable format.
# MAGIC 
# MAGIC The log files that we use for this lab are in the [Apache Common Log Format (CLF)](http://httpd.apache.org/docs/1.3/logs.html#common). The log file entries produced in CLF will look something like this:
# MAGIC `127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839`
# MAGIC  
# MAGIC A summary is provided below, describing each part of the log entry.
# MAGIC * `127.0.0.1`
# MAGIC This is the IP address (or host name, if available) of the client (remote host) which made the request to the server.
# MAGIC  
# MAGIC * `-`
# MAGIC The "hyphen" in the output indicates that the requested piece of information (user identity from remote machine) is not available.
# MAGIC  
# MAGIC * `-`
# MAGIC The "hyphen" in the output indicates that the requested piece of information (user identity from local logon) is not available.
# MAGIC  
# MAGIC * `[01/Aug/1995:00:00:01 -0400]`
# MAGIC The time that the server finished processing the request. The format is:
# MAGIC `[day/month/year:hour:minute:second timezone]`
# MAGIC   * day = 2 digits
# MAGIC   * month = 3 letters
# MAGIC   * year = 4 digits
# MAGIC   * hour = 2 digits
# MAGIC   * minute = 2 digits
# MAGIC   * second = 2 digits
# MAGIC   * zone = (\+ | \-) 4 digits
# MAGIC  
# MAGIC * `"GET /images/launch-logo.gif HTTP/1.0"`
# MAGIC This is the first line of the request string from the client. It consists of a three components: the request method (e.g., `GET`, `POST`, etc.), the endpoint (a [Uniform Resource Identifier](http://en.wikipedia.org/wiki/Uniform_resource_identifier)), and the client protocol version.
# MAGIC  
# MAGIC * `200`
# MAGIC This is the status code that the server sends back to the client. This information is very valuable, because it reveals whether the request resulted in a successful response (codes beginning in 2), a redirection (codes beginning in 3), an error caused by the client (codes beginning in 4), or an error in the server (codes beginning in 5). The full list of possible status codes can be found in the HTTP specification ([RFC 2616](https://www.ietf.org/rfc/rfc2616.txt) section 10).
# MAGIC  
# MAGIC * `1839`
# MAGIC The last entry indicates the size of the object returned to the client, not including the response headers. If no content was returned to the client, this value will be "-" (or sometimes 0).
# MAGIC  
# MAGIC Note that log files contain information supplied directly by the client, without escaping. Therefore, it is possible for malicious clients to insert control-characters in the log files, *so care must be taken in dealing with raw logs.*

# COMMAND ----------

import re
from pyspark.sql import Row

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)'

# Returns a dictionary containing the parts of the Apache Access Log.
def parse_apache_log_line(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        # Optionally, you can change this to just ignore if each line of data is not critical.
        #   Corrupt data is common when writing to files.
        raise Exception("Invalid logline: %s" % logline)
    return Row(
        ipAddress    = match.group(1),
        clientIdentd = match.group(2),
        userId       = match.group(3),
        dateTime     = match.group(4),
        method       = match.group(5),
        endpoint     = match.group(6),
        protocol     = match.group(7),
        responseCode = int(match.group(8)),
        contentSize  = long(match.group(9)))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 3:** Load the log lines into a Spark RDD (Resilient Distributed Dataset).

# COMMAND ----------

access_logs = (sc.textFile(DBFS_SAMPLE_LOGS_FOLDER)
               # Call the parse_apace_log_line function on each line.
               .map(parse_apache_log_line)
               # Caches the objects in memory since they will be queried multiple times.
               .cache())
# An action must be called on the RDD to actually cache it.
access_logs.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 4:** Calculate statistics on the sizes of the content returned by the GET requests.

# COMMAND ----------

# Create the content sizes rdd.
content_sizes = (access_logs
                 .map(lambda row: row.contentSize)
                 .cache()) # Cache this as well since it will be queried many times.

# COMMAND ----------

# Compute the average content size.
average_content_size = content_sizes.reduce(lambda x, y: x + y) / content_sizes.count()
average_content_size

# COMMAND ----------

# Compute the minimum content size.
min_content_size = content_sizes.min()
min_content_size

# COMMAND ----------

# // Compute the maximum content size.
max_content_size = content_sizes.max()
max_content_size

# COMMAND ----------

print "Content Size Statistics:\n  Avg: %s\n  Min: %s\n  Max: %s" % (average_content_size, min_content_size, max_content_size)

# COMMAND ----------

# MAGIC %md ### **Step 5:** Compute statistics on the response codes.

# COMMAND ----------

# First, calculate the response code to count pairs.
response_code_to_count_pair_rdd = (access_logs
                                   .map(lambda row: (row.responseCode, 1))
                                   .reduceByKey(lambda x, y: x + y))
response_code_to_count_pair_rdd

# COMMAND ----------

# View the responseCodeToCount by calling take() on the RDD - which outputs an array of tuples.
# Notice the use of take(100) - just in case bad data may have slipped in and there are too many response codes.
response_code_to_count_array = response_code_to_count_pair_rdd.take(100)
response_code_to_count_array

# COMMAND ----------

# To call display(), the RDD of tuples must be converted to a DataFrame.
# A simple map can accomplish that.
response_code_to_count_row_rdd = response_code_to_count_pair_rdd.map(lambda (x, y): Row(response_code=x, count=y))
response_code_to_count_data_frame = sqlContext.createDataFrame(response_code_to_count_row_rdd)

# COMMAND ----------

# Now, display can be called on the resulting DataFrame.
#   For this display, a Pie Chart is chosen by selecting that icon.
#   Then, "Plot Options..." was used to make the response_code the key and count as the value.
display(response_code_to_count_data_frame)

# COMMAND ----------

# MAGIC %md #### NOTE: Instead of converting to a DataFrame for displaying, Spark SQL could also have been used.
# MAGIC * At the end of this notebook, it's shown how to register this table for Spark SQL.
# MAGIC * See the **Log Analysis in SQL** Notebook for more details on writing sql queries.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 6:** View a list of IPAddresses that has accessed the server more than N times.

# COMMAND ----------

# Change this number as it makes sense for your data set.
n = 100

# COMMAND ----------

ip_addresses_rdd = (access_logs
                    .map(lambda log: (log.ipAddress, 1))
                    .reduceByKey(lambda x, y : x + y)
                    .filter(lambda s: s[1] > n)
                    .map(lambda s: Row(ip_address = s[0]))) # Create a Row so this can be converted to a DataFrame.
ip_addresses_dataframe = sqlContext.createDataFrame(ip_addresses_rdd)

# COMMAND ----------

# Display this list of IPAddresses in a chart with scrolling is a nice way to present this data.
display(ip_addresses_dataframe)

# COMMAND ----------

# MAGIC %md #### **Tip:** To see how an rdd is computed, use the **toDebugString()** function.

# COMMAND ----------

print ip_addresses_dataframe.rdd.toDebugString()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 7:** Explore statistics about the endpoints. 

# COMMAND ----------

# Calculate the number of hits for each endpoint.
endpoint_counts_rdd = (access_logs
                       .map(lambda log: (log.endpoint, 1))
                       .reduceByKey(lambda x, y: x + y)
                       .map(lambda s: Row(endpoint = s[0], num_hits = s[1])))
endpoint_counts_dataframe = sqlContext.createDataFrame(endpoint_counts_rdd)

# COMMAND ----------

# Display a plot of the distribution of the number of hits across the endpoints.
display(endpoint_counts_dataframe)

# COMMAND ----------

# Drill down to the top endpoints.
top_endpoints_array = endpoint_counts_rdd.takeOrdered(10, lambda row: -1 * row.num_hits)
top_endpoints_dataframe = sqlContext.createDataFrame(sc.parallelize(top_endpoints_array))
display(top_endpoints_dataframe)
                            

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### **Extra: Use SQL in a Python Notebook**
# MAGIC * A dataframe can registered as a temporary SQL Table.
# MAGIC * Then you can do SQL queries against the data in sql cells in this Python notebook.
# MAGIC * Results from select statements in SQL are automatically displayed.

# COMMAND ----------

# Create a DataFrame from the rdd of Row's.
access_logs_dataframe = sqlContext.createDataFrame(access_logs)

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS SQLApacheAccessLogsTable

# COMMAND ----------

# Save DataFrame as Table
access_logs_dataframe.write.saveAsTable("SQLApacheAccessLogsTable")

# COMMAND ----------

# MAGIC %sql describe SQLApacheAccessLogsTable

# COMMAND ----------

# MAGIC %sql SELECT * FROM SQLApacheAccessLogsTable limit 10;