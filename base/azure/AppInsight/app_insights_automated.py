# Databricks notebook source
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
commonLoggingFormat = '%(levelname)s - %(name)s - %(message)s'
basicLoggingFormat = '%(asctime)s - ' + commonLoggingFormat
logging.basicConfig(format=basicLoggingFormat, level=logging.INFO)

# Set logging level higher for some libraries, otherwise databricks logs drowned with useless info
loggingExclusionList = ("py4j.java_gateway", "opencensus.ext.azure.common.transport")
for _ in (loggingExclusionList):
    logging.getLogger(_).setLevel(logging.WARNING)

appInsightsHandler = AzureLogHandler(connection_string='InstrumentationKey=25862283-649a-42cd-aaff-c4d81a555a88')
appInsightsHandler.setFormatter(logging.Formatter(commonLoggingFormat))
logger.addHandler(appInsightsHandler)


partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
n = 100000 * partitions

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

logger.info("Starting from automated job 1234")
count = spark.sparkContext.parallelize(
    range(1, n + 1), partitions).map(f).reduce(add)
logger.info("Done computing automated job 1234.")
print("Pi is roughly %f" % (4.0 * count / n))


success_message = waiting_function(60)
logger.info(success_message)

# COMMAND ----------

