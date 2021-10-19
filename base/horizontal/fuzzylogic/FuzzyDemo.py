# Databricks notebook source
# MAGIC %md
# MAGIC ***Fuzzy logic is a form of many-valued logic in which the truth values of variables may be any real number between 0 and 1. It is employed to handle the concept of partial truth, where the truth value may range between completely true and completely false. By contrast, in Boolean logic, the truth values of variables may only be the integer values 0 or 1. Furthermore, when linguistic variables are used, these degrees may be managed by specific (membership) functions***
# MAGIC 
# MAGIC One of the application of Fuzzy logic we will see below is to identify duplicate records as a part of the MDM process.
# MAGIC 
# MAGIC 
# MAGIC * [**Fuzzy Logic in MDM Analytics**](https://en.wikipedia.org/wiki/Fuzzy_logic) is the use of Fuzzy Logic to identfy duplicate records and is...  
# MAGIC   * Built on top of Databricks Platform
# MAGIC   * Uses a fuzzywuzzy python package implementation to analysis a internally generated people address dataset   
# MAGIC * This demo...  
# MAGIC   * demonstrates a simple MDM de-dup identification analysis workflow.  We use internally generated dataset.

# COMMAND ----------

# DBTITLE 1,0. Setup Databricks Spark cluster:
# MAGIC %md
# MAGIC 
# MAGIC **Create** a cluster by...  
# MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` 
# MAGIC   - Enter any text, i.e `demo` into the cluster name text box
# MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
# MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
# MAGIC   
# MAGIC **Attach** this notebook to your cluster by...   
# MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 
# MAGIC   
# MAGIC **Add FuzzyWuzzy to your cluster**
# MAGIC 
# MAGIC - Right click in Workspace, click 'Create' --> 'Library', then select PyPi and type 'fuzzywuzzy', then attach to your cluster._

# COMMAND ----------

# DBTITLE 1,Step1: Ingest patient Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - Dataset is internally generated and hosted at **/FileStore/tables/people.csv**

# COMMAND ----------

df=spark.read.format('csv').option("header", "true").load('/FileStore/tables/people.csv')
display(df)

# COMMAND ----------

# MAGIC %md For production workflows, disable broadcast joins temporarily. Broadcast joins don't work well for cartesian products because the workers get so much broadcast data they get stuck in an infinite garbage collection loop and never finish._
# MAGIC 
# MAGIC __Remember to turn this back on when the query finishes.__

# COMMAND ----------

#%sql set spark.sql.autoBroadcastJoinThreshold =0

# COMMAND ----------

joined_df = df.select("firstName","lastName","street","city","state","zipcode").crossJoin(
  df\
    .select("firstName","lastName","street","city","state","zipcode")\
    .withColumnRenamed("firstName","firstName1")\
    .withColumnRenamed("lastName","lastName1")\
    .withColumnRenamed("street","street1")\
    .withColumnRenamed("city","city1")\
    .withColumnRenamed("state","state1")\
    .withColumnRenamed("zipcode","zipcode1")
  ).fillna("UKN")

display(joined_df)

# COMMAND ----------

joined_df.explain() # Note: RoundRobin Partitioning

# COMMAND ----------

#%sql set spark.sql.autoBroadcastJoinThreshold =1

# COMMAND ----------

# MAGIC %md ### Step2: Enrich the data to get additional insigts to People dataset
# MAGIC - We create python UDF to make use of fuzzywuzzy package.

# COMMAND ----------

# verify library is installed
from fuzzywuzzy import fuzz

# import pyspark class Row from module sql
from pyspark.sql import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md _Functions used for fuzzywuzzy based matching._

# COMMAND ----------

def get_ratio(s1, s2):
  if s1 and s2:
    ratio = fuzz.ratio(s1.lower(), s2.lower())
  else: 
    ratio = 0
  return ratio

def get_partial_ratio(s1, s2):
  if s1 and s2:
    partial_ratio = fuzz.partial_ratio(s1.lower(), s2.lower())
  else: 
    partial_ratio = 0
  return partial_ratio

def get_token_sort_ratio(s1, s2):
  if s1 and s2:
    token_sort_ratio = fuzz.token_sort_ratio(s1.lower(), s2.lower())
  else: 
    token_sort_ratio = 0
  return token_sort_ratio

# COMMAND ----------

# MAGIC %md _Define UDFs for distributed computing._

# COMMAND ----------

getFuzzRatio = udf(get_ratio, IntegerType())
getFuzzPartialRatio = udf(get_partial_ratio, IntegerType())
getFuzzTokenSortRatio = udf(get_token_sort_ratio, IntegerType())
lastNameStartsWith = udf(lambda x: x.strip().upper()[0], StringType())

# COMMAND ----------

# MAGIC %md _Define UDFs for distributed computing._

# COMMAND ----------

df_withRatios = joined_df \
                .withColumn("ratio_first", getFuzzRatio("firstName", "firstName1")) \
                .withColumn("part_ratio_first", getFuzzPartialRatio("firstName", "firstName1")) \
                .withColumn("token_ratio_first", getFuzzTokenSortRatio("firstName", "firstName1")) \
                .withColumn("ratio_zip", getFuzzRatio("zipcode", "zipcode1")) \
                .withColumn("last_name_starts_with", lastNameStartsWith("lastName1"))
display(df_withRatios)

# COMMAND ----------

# MAGIC %md _Filter Out Matches below 90._

# COMMAND ----------

df_withRatios.createOrReplaceTempView("mdm_cross_join_ratios")

matched_df = spark.sql("""
          select firstName, lastName, firstName1, lastName1, street, city, state, zipcode, last_name_starts_with
          from mdm_cross_join_ratios
          where ratio_first > 90
          or part_ratio_first > 90
          or token_ratio_first > 90         
          """)

display(matched_df)

# COMMAND ----------

matched_df1 = spark.sql("""
          select firstName, lastName,zipcode, firstName1, lastName1,zipcode1, street, city, state, last_name_starts_with
          from mdm_cross_join_ratios
          where ratio_first > 95
          or part_ratio_first > 95
          or token_ratio_first > 95    
          or ratio_zip > 95
          """)

display(matched_df1)

# COMMAND ----------

# MAGIC %md ### Additional Documentation for Similarity Scoring
# MAGIC 
# MAGIC Spark comes packaged with a few useful functions. 
# MAGIC 
# MAGIC __Levenshtein Function__ (http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$@levenshtein(l:org.apache.spark.sql.Column,r:org.apache.spark.sql.Column):org.apache.spark.sql.Column) : This could be useful because it can tell you the edit distance between 2 strings.  This is useful if someone misspells a name.  It could show you that there is a distance of 1 between Michael and Micheal.
# MAGIC 
# MAGIC __Soundex Function__ (http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$@soundex(e:org.apache.spark.sql.Column):org.apache.spark.sql.Column) : This is useful because it can tell you if 2 things are the same if they sound alike. For example, this will return the same value for each entry:
# MAGIC `select soundex('Sean'), soundex('Shawn'), soundex('Shaun')`