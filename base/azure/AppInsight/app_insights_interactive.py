# Databricks notebook source
# local copy of instrumentation id

corr_id = "25862283-649a-42cd-aaff-c4d81a555a88"

# COMMAND ----------

#install init script in dbfs to use with automated clusters

init_script_path = "dbfs:/int_scripts_dir/pipscript.sh"

dbutils.fs.rm(init_script_path)

dbutils.fs.put(init_script_path, """
#!/bin/bash
/databricks/python3/bin/python -m pip install opencensus-ext-azure==1.0.0
""")

# COMMAND ----------



# COMMAND ----------

from opencensus.ext.azure.log_exporter import AzureLogHandler
from pyspark.sql import SparkSession
from random import random
from operator import add
import logging
import time
import sys
import os


def waiting_function(amount_of_time):
    time.sleep(amount_of_time)
    return "done sleeping"

# Logger and basic config - different config for AppInsight to avoid duplicate timestamp in message
logger = logging.getLogger("py4j")
# log4jLogger = sc._jvm.org.apache.log4j
# LOGGER = log4jLogger.LogManager.getLogger(__name__)

commonLoggingFormat = '%(levelname)s - %(name)s - %(message)s'
basicLoggingFormat = '%(asctime)s - ' + commonLoggingFormat
logging.basicConfig(format=basicLoggingFormat, level=logging.INFO)

# Set logging level higher for some libraries, otherwise databricks logs drowned with useless info
loggingExclusionList = ("py4j.java_gateway", "opencensus.ext.azure.common.transport")
for _ in (loggingExclusionList):
    logging.getLogger(_).setLevel(logging.WARNING)

# Logging configuration for AppInsights
try:
    instrumentationKey = os.environ["CORRELATION_ID"]
except:
    logger.error("Unable to read environement variables")
    raise

appInsightsHandler = AzureLogHandler(connection_string='InstrumentationKey=25862283-649a-42cd-aaff-c4d81a555a88')
appInsightsHandler.setFormatter(logging.Formatter(commonLoggingFormat))
logger.addHandler(appInsightsHandler)


partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
n = 100000 * partitions

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

logger.info("Starting interactive 1234 ")
count = spark.sparkContext.parallelize(
    range(1, n + 1), partitions).map(f).reduce(add)
logger.info("Done computing interactive 1234")
print("Pi is roughly %f" % (4.0 * count / n))

success_message = waiting_function(60)
logger.info(success_message)