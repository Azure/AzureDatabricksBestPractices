# Databricks notebook source
adls_base = "dbfs:/mnt/ndwpocdl/"
incoming_dir = "incoming/"

# COMMAND ----------

fact_alarm_dl = adls_base+incoming_dir+"fact_alarm/fact_alarm.txt"
#fact_alarm_dl = adls_base+"fact_alarm.txt"

fact_alarm_df = (spark.read
        .option("delimiter", ",") #This is how we could pass in a Tab or other delimiter.
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",") #sep, quote, escape are needed for escaping the ',' char in cells
        .option("quote", '"')
        .option("escape", '"')
        .csv(fact_alarm_dl)
)

# COMMAND ----------

#isplay(fact_alarm_df.describe())
display(fact_alarm_df)