# Databricks notebook source
# MAGIC %md
# MAGIC # **Create Sample Logs**
# MAGIC Use this notebook to randomly generate sample Apache access logs that are saved to S3.
# MAGIC 
# MAGIC Skip this notebook if you have your own production Apache access logs to analyze.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 0:** Access Sample Log File saved to DBFS
# MAGIC 
# MAGIC We generated a sample log file with the below steps, and saved it to DBFS for your convenience. You can follow the steps below to generate your own sample log file, or use the file saved on DBFS to quickly get started with log analysis.

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/sample_logs/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 1:** Change these parameters for your own preferences.

# COMMAND ----------

DBFS_SAMPLE_LOGS_FOLDER = "/analysis/sample_logs"  # Optionally change this path if you would like.
TOTAL_LOG_LINES_TO_GENERATE = 100000

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 2:** Create a function to randomly generate log lines.
# MAGIC Optional: Make this more random if you'd like.

# COMMAND ----------

import random
def generate_log_line():
  return "%s %s %s [%s] \"%s %s HTTP/1.1\" %s %s" % (
    # IPAddress
    random.choice(["127.0.0.1", "1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4"]),
    # Client IdentD
    "-",
    # User ID 
    random.choice(["-", "-", "-", "user1", "user2"]),
    # Date
    random.choice(["21/Jan/2014:10:00:00 -0200", "21/Feb/2014:10:00:00 -0300", "21/Mar/2014:10:00:00 -0400", "21/Apr/2014:10:00:00 -0500", "21/May/2014:10:00:00 -0600", "21/Jun/2014:10:00:00 -0700", "21/Jul/2014:10:00:00 -0800", ]),
    # Commands
    random.choice(["GET", "GET", "GET", "GET", "POST", "PUT"]),
    # Endpoint
    "/endpoint_%02d" % random.randint(0, 1000),
    # Response Code
    random.choice([200, 200, 200, 200, 200, 401, 500]),
    # Content Size
    random.randint(0, 500))

# COMMAND ----------

generate_log_line()

# COMMAND ----------

# MAGIC %md ### **Step 3:** Write the random log lines to files.

# COMMAND ----------

sc.parallelize(range(TOTAL_LOG_LINES_TO_GENERATE)).map(lambda i: generate_log_line()).saveAsTextFile(DBFS_SAMPLE_LOGS_FOLDER)