# Databricks notebook source
# DBTITLE 1,Sample Delta Table Ingestion
# MAGIC %md
# MAGIC <img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" width="60%">
# MAGIC 
# MAGIC * This notebook will walk you through the basics of downloading sample data, ingesting some of it into a Delta table, then emulating new and changed data and ingesting that additional data into the Delta table.
# MAGIC * You can run it multiple times and it will continue to emulate new/changed data on each run.
# MAGIC * **To clear all the stored data, manually execute the last cell which will remove any persisted tables and files stored on disk.**
# MAGIC 
# MAGIC * * * 
# MAGIC 
# MAGIC ##### Steps performed below
# MAGIC 1. Download sample data from https://opendata.socrata.com/Fun/Top-1-000-Songs-To-Hear-Before-You-Die/ed74-c6ni, storing data locally (takes a few seconds)
# MAGIC 1. Move downloaded file off the driver, move to DBFS (emulating how it would appear if cloud storage bucket was mounted already)
# MAGIC 1. Split part of raw data out to emulate an initial batch load
# MAGIC 1. Create Table to point to Inbound CSV directory
# MAGIC 1. Create Delta Table to store "SILVER" data, which will house merged inbound data
# MAGIC 1. Insert initial load data from Inbound CSV to SILVER Delta Table
# MAGIC 1. Split some more raw data off, update dates on it, write it to Inbound CSV directory, emulating new data arriving
# MAGIC 1. Merge newly arriving Inbound CSV data into SILVER Delta Table
# MAGIC 1. View some information about the Delta table and stored information (changed data, etc.)
# MAGIC 1. Cleanup, remove files created through this notebook (WILL NOT AUTOMATICALLY RUN)

# COMMAND ----------

# DBTITLE 1,Download sample data
# MAGIC %sh wget -O songs_rows.csv https://opendata.socrata.com/api/views/ed74-c6ni/rows.csv?accessType=DOWNLOAD

# COMMAND ----------

# DBTITLE 1,Move downloaded data off driver node, to DBFS (as if it were already in a cloud storage bucket)
# CREATE A DBFS FOLDER TO HOUSE DATA FOR DEMO
#raw_folder = "dbfs:/user/alistair/delta-ingest-demo/consumer-complaints/raw"
raw_folder = "dbfs:/user/alistair/delta-ingest-demo/songs/raw"
dbutils.fs.mkdirs(raw_folder)

# MOVE THE DOWNLOADED FILE FROM THE DRIVER NODE TO DBFS (TO EMULATE WHAT IT WOULD LOOK LIKE IF CLOUD-STORAGE MOUNTED RATHER THAN DOWNLOADED)
dbutils.fs.mv("file:/databricks/driver/songs_rows.csv", raw_folder)

# SHOW THE CONTENTS OF THE DBFS FOLDER
display(dbutils.fs.ls(raw_folder))

# COMMAND ----------

# DBTITLE 1,View a sample of the downloaded file
# MAGIC %fs head dbfs:/user/alistair/delta-ingest-demo/songs/raw/songs_rows.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS alistair_demo;

# COMMAND ----------

# DBTITLE 1,Load a portion of the data to use as a single input file, Create an empty Delta table based on the source data layout
from os import path
from datetime import datetime
from pyspark.sql.functions import col

# READ RAW FOLDER DATA AS A SPARK DATAFRAME
dfRawData = spark.read.format('csv').options(header='true', inferSchema='true', multiLine='true').load(raw_folder)
exprs = [col(column).alias(column.replace(" ", "_")) for column in dfRawData.columns]
dfRawData = dfRawData.select(*exprs)

# CREATE 2 DATAFRAMES, BATCH 1 TO EMULATE AN INITIAL FEED OF DATA, AND THE REMAINDER TO BE DISCARDED
(dfBatch1, dfJunk) = dfRawData.randomSplit([0.75, 0.25])

# CREATE TARGET FOLDER, WRITE OUT SEGMENTED DATASET
incremental_root_folder = "dbfs:/user/alistair/delta-ingest-demo/songs/incrementals"

partition_name = datetime.now().strftime("%Y%m%d_%H%M%S")
write_to_path = path.join(incremental_root_folder, partition_name)
temp_table_name = "ingest_csv_{}".format(partition_name)
dfBatch1.write.csv(write_to_path, mode="overwrite")
dfBatch1.registerTempTable(temp_table_name)

print("temp table: ", temp_table_name)

# CREATE A TABLE DEFINITION OFF AN EMPTY VERSION OF THE DATAFRAME, POINTING TO THE BASE INCREMENTAL FOLDER (all inbound data)
#dfBatch1.filter("1=2").write.format("csv").mode("overwrite").option("path", incremental_root_folder).saveAsTable("alistair_demo.ingest_csv_raw")
delta_folder = "dbfs:/user/alistair/delta-ingest-demo/songs/delta"
sqlCreateDeltaTable = "CREATE TABLE IF NOT EXISTS alistair_demo.songs_delta \
  USING delta \
  LOCATION '{deltaLocation}' \
  AS SELECT *, CAST(CURRENT_TIMESTAMP as timestamp) AS etl_insert_dttm, CAST(CURRENT_TIMESTAMP as timestamp) AS etl_update_dttm FROM {tempTable} \
  WHERE 1=2 ".format(deltaLocation = delta_folder, tempTable = temp_table_name)

spark.sql(sqlCreateDeltaTable)


# COMMAND ----------

# DBTITLE 1,Functions to help with column manipulation
# HELPER FUNCTIONS TO SIMPLIFY OTHER CODE BLOCKS

# COLLECT COLUMN LIST AND PREPARE FOR MERGE STATEMENT (NECESSARY DUE TO APPENDED TIMESTAMP COLUMN), GIVEN A COLUMN LIST (FROM A DATAFRAME)
# ALSO HAS TWO ROW COMPARISON METHODS (HASH ALL COLUMNS, SEPARATE OR COMPARISONS FOR EACH COLUMN)
def prepareColumnSQL(dfColList, rowCompareType):
  col_list = [column.replace(" ", "_") for column in dfColList]
  
  upd_list = ""
  for column in col_list:
    upd_list += "`{colname}` = ^SOURCE^.`{colname}`, ".format(colname = column)

  if upd_list.strip()[-1] == ",":
    upd_list = upd_list.strip()[:-1]

  upd_list += ", `etl_update_dttm` = CAST(CURRENT_TIMESTAMP as timestamp)"

  if rowCompareType == "columns":
    upd_qual = ""
    for column in col_list:
      upd_qual += "^TARGET^.`{colname}` <> ^SOURCE^.`{colname}` OR ".format(colname = column)

    if upd_qual.strip()[-2:] == "OR":
      upd_qual = upd_qual.strip()[:-2]

  if rowCompareType == "hash":
    upd_qual = "hash({tgt}) <> hash({src})".format(
      tgt = ", ".join(["^TARGET^.`{0}`".format(column) for column in col_list]),
      src = ", ".join(["^SOURCE^.`{0}`".format(column) for column in col_list]),
      )
    
  ins_list = ""
  for column in col_list:
    ins_list += "`{colname}`, ".format(colname = column)

  if ins_list.strip()[-1] == ",":
    ins_list = ins_list.strip()[:-1]

  ins_list = "({columns}, `etl_insert_dttm`, `etl_update_dttm`) VALUES ({columns}, CAST(CURRENT_TIMESTAMP as timestamp), CAST(CURRENT_TIMESTAMP as timestamp))".format(columns = ins_list)
  
  return upd_list, upd_qual, ins_list


# ELIMINATE FORMATTING FROM SQL FOR LOG OUTPUT
def cleanSQLText(sqlText):
  return " ".join(sqlText.split(" ")).strip()


# COMMAND ----------

# DBTITLE 1,Create Merge statement to load inbound CSV data to Delta table (SILVER table)
# PREPARE MERGE STATEMENT, "hash" TO COMPARE HASH OF ALL COLUMNS TO ASSESS CHANGED RECORDS, "columns" TO INDIVIDUALLY COMPARE COLUMNS
upd_list, upd_qual, ins_list = prepareColumnSQL(dfRawData.columns, "hash")

sqlUpsertToDeltaTable = "\
  MERGE INTO alistair_demo.songs_delta \n \
  USING {tempTable} AS songs_incrementals \n \
    ON \n \
      songs_delta.`title` = songs_incrementals.`title` \n \
      and songs_delta.`artist` = songs_incrementals.`artist` \n \
      and songs_delta.`theme` = songs_incrementals.`theme` \n\n \
  WHEN MATCHED AND ({upd_qual}) THEN UPDATE SET {upd_list} \n\n \
  WHEN NOT MATCHED THEN INSERT {ins_list} \n \
  ".format( tempTable = temp_table_name
           , upd_list = upd_list.replace("^SOURCE^", "songs_incrementals")
           , ins_list = ins_list
           , upd_qual = upd_qual.replace("^TARGET^", "songs_delta").replace("^SOURCE^", "songs_incrementals")
          )

print( "USING SQL:\n{sql}".format(sql = cleanSQLText(sqlUpsertToDeltaTable)) )
spark.sql(sqlUpsertToDeltaTable)

sqlDropTempTable = "DROP TABLE {tempTable}".format(tempTable = temp_table_name)
spark.sql(sqlDropTempTable)

# COMMAND ----------

# MAGIC %sql OPTIMIZE alistair_demo.songs_delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC show create table alistair_demo.songs_delta 

# COMMAND ----------

# MAGIC %sql select * from alistair_demo.songs_delta

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/alistair/delta-ingest-demo/songs/delta

# COMMAND ----------

# DBTITLE 1,Songs by Theme over the years
# MAGIC %sql
# MAGIC select year, theme, count(*) as song_count from alistair_demo.songs_delta
# MAGIC where year >= 1964 and lower(theme) in ('life and death', 'love', 'politics and protest', 'party songs')
# MAGIC group by year, theme order by year asc, theme asc

# COMMAND ----------

display(dbutils.fs.ls(incremental_root_folder))

# COMMAND ----------

# DBTITLE 1,Using downloaded data, gather another sample to use as new or changed data
# BRING IN MORE RANDOM DATA TO USE AS INCREMENTAL LOAD

# CREATE 2 DATAFRAMES, BATCH 1 TO EMULATE A CHANGED FEED OF DATA, AND THE REMAINDER TO BE DISCARDED
(dfBatch2, dfJunk) = dfRawData.randomSplit([0.35, 0.65])

partition_name = datetime.now().strftime("%Y%m%d_%H%M%S")
write_to_path = path.join(incremental_root_folder, partition_name)
temp_table_name = "ingest_csv_{}".format(partition_name)
dfBatch2.write.csv(write_to_path, mode="overwrite")
dfBatch2.registerTempTable(temp_table_name)

print("temp table: ", temp_table_name)

# COMMAND ----------

# DBTITLE 1,Manipulate some of the inbound data to ensure that there will be some changed rows to be upserted
# THIS IS THE SAME MERGE STATEMENT AS ABOVE FOR THE INITIAL INSERT - THE ONLY THING CHANGING IS FORCING MANUAL UPDATES ON THE "SOURCE" DATA

# PREPARE MERGE STATEMENT, "hash" TO COMPARE HASH OF ALL COLUMNS TO ASSESS CHANGED RECORDS, "columns" TO INDIVIDUALLY COMPARE COLUMNS
upd_list, upd_qual, ins_list = prepareColumnSQL(dfRawData.columns, "hash")

manual_updates_override = "(SELECT THEME, TITLE, ARTIST, YEAR, \n \
  CASE WHEN YEAR < 1990 THEN SPOTIFY_URL || '_updated' ELSE SPOTIFY_URL END AS SPOTIFY_URL \n \
  FROM {tempTable})".format(tempTable = temp_table_name)

sqlUpsertToDeltaTable = "\
  MERGE INTO alistair_demo.songs_delta \n \
  USING {tempTable} AS songs_incrementals \n \
    ON \n \
      songs_delta.`title` = songs_incrementals.`title` \n \
      and songs_delta.`artist` = songs_incrementals.`artist` \n \
      and songs_delta.`theme` = songs_incrementals.`theme` \n\n \
  WHEN MATCHED AND ({upd_qual}) THEN UPDATE SET {upd_list} \n\n \
  WHEN NOT MATCHED THEN INSERT {ins_list} \n \
  ".format( tempTable = manual_updates_override # THIS WAS UPDATED FROM THE ORIGINAL
           , upd_list = upd_list.replace("^SOURCE^", "songs_incrementals")
           , ins_list = ins_list
           , upd_qual = upd_qual.replace("^TARGET^", "songs_delta").replace("^SOURCE^", "songs_incrementals")
          )

print( "USING SQL:\n{sql}".format(sql = cleanSQLText(sqlUpsertToDeltaTable)) )
spark.sql(sqlUpsertToDeltaTable)

sqlDropTempTable = "DROP TABLE {tempTable}".format(tempTable = temp_table_name)
spark.sql(sqlDropTempTable)

# COMMAND ----------

# MAGIC %sql OPTIMIZE alistair_demo.songs_delta

# COMMAND ----------

# DBTITLE 1,Show count of changed rows across the 2 simulated ETL runs
# MAGIC %sql
# MAGIC select left(cast(etl_insert_dttm as varchar(100)), 19) as etl_insert_dttm, left(cast(etl_update_dttm as varchar(100)), 19) as etl_update_dttm, count(*)
# MAGIC from alistair_demo.songs_delta
# MAGIC group by left(cast(etl_insert_dttm as varchar(100)), 19), left(cast(etl_update_dttm as varchar(100)), 19) order by 1 desc, 2 desc

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL alistair_demo.songs_delta

# COMMAND ----------

# DBTITLE 1,View the change history log on the Delta table
# MAGIC %sql DESCRIBE HISTORY alistair_demo.songs_delta

# COMMAND ----------

# DBTITLE 1,View changed rows over time through Delta history (MANUAL)
# ##### CAUTION ##### THIS IS NOT A PERFORMANT OPERATION, BUT MEANT TO DISPLAY THE CAPABILITY TO SEE DIFFERENT VERSIONS OF "THE SAME ROW" AFTER UPSERT/MERGE OPERATIONS
# THIS IS NOT A SUITABLE REPLACEMENT FOR A BETTER ARCHITECTURAL APPROACH SUCH AS HAVING AN APPEND-ONLY CDC TABLE AND "LATEST SNAPSHOT" TABLE

from pyspark.sql.functions import *
from functools import reduce
from pyspark.sql import DataFrame

from multiprocessing.pool import ThreadPool
from numpy import min

tableName = "alistair_demo.songs_delta"
table_pks = ["theme", "title", "artist"]

upserted_row_example = spark.sql( "select {pks} from {tableName} where etl_update_dttm <> etl_insert_dttm order by etl_update_dttm desc limit 1"
                                 .format(tableName = tableName, pks = ", ".join(table_pks)) )
howmany_updated_rows = upserted_row_example.count()

# function to gather details from a delta table to bring together
def getVersionData(ver, tableName, pk_values):
  sqlQueryDynamic = "select *, {version} as databricks_delta_version from {tableName} version as of {version} where {clause}" \
    .format(version = ver, tableName = tableName,
            clause = " and ".join(["{0} = '{1}'".format(key, val) for key, val in pk_values.items()])
           )
  dfX = spark.sql(sqlQueryDynamic)
  return dfX


if howmany_updated_rows > 0:

  pk_values = {}
  for pk in table_pks:
    pk_values[pk] = upserted_row_example.collect()[0][pk]
  
  version_list = spark.sql( "describe history {tableName}".format(tableName = tableName) ) \
    .select(["version", "timestamp"]) \
    .where("version is not null") \
    .distinct() \
    .orderBy("version")
  
  howmany_table_versions = version_list.count()
  if howmany_table_versions > 0:
    ver_array = [int(row["version"]) for row in version_list.collect()]
    
    getMethod = "thread" # loop -or- thread
    
    if getMethod == "thread":
      pool = ThreadPool(min([15, howmany_table_versions])) # cap max threads in case of high table version count
      dfs = pool.map(lambda ver: getVersionData(ver, tableName, pk_values), ver_array)
    
    if getMethod == "loop":
      dfs = []
      for ver in ver_array:
        dfs.append( getVersionData(ver, tableName, pk_values) )
  
    unionDF = reduce(DataFrame.unionAll, dfs)
    col_list = unionDF.columns
    col_list.remove("databricks_delta_version")

    dfGrouped = unionDF \
            .groupBy(col_list) \
            .agg({"databricks_delta_version":"min"}) \
            .withColumnRenamed("min(databricks_delta_version)", "as_of_databricks_delta_version") \
            .orderBy(desc("as_of_databricks_delta_version"))
    
    display(
      dfGrouped
        .join(version_list, dfGrouped.as_of_databricks_delta_version == version_list.version)
        .select(dfGrouped["*"], version_list["timestamp"].alias("as_of_databricks_timestamp"))
      )
    

# COMMAND ----------

# DBTITLE 1,Stop the notebook before the cleanup cell, in case of a "run all"
dbutils.notebook.exit("stop") 

# COMMAND ----------

# DBTITLE 1,Cleanup operations
## CLEANUPS / REMOVE CREATED FILES (LEAVING ANY FOLDERS THAT MAY HAVE OTHER CONTENTS IN THEM)
from os import path

raw_folder = "dbfs:/user/alistair/delta-ingest-demo/songs/raw"
file_to_remove = path.join(raw_folder, "songs_rows.csv")
dbutils.fs.rm(file_to_remove, True)

incremental_root_folder = "dbfs:/user/alistair/delta-ingest-demo/songs/incrementals"
dbutils.fs.rm(incremental_root_folder, True)

spark.sql("DROP TABLE IF EXISTS alistair_demo.songs_delta")

delta_folder = "dbfs:/user/alistair/delta-ingest-demo/songs/delta"
dbutils.fs.rm(delta_folder, True)

# COMMAND ----------

