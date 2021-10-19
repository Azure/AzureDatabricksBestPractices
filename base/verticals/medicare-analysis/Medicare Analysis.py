# Databricks notebook source
# MAGIC %sql 
# MAGIC -- Name of brand / specific drug. 
# MAGIC select Name_of_Associated_Covered_Drug_or_Biological1, Name_of_Associated_Covered_Drug_or_Biological2, 
# MAGIC NDC_of_Associated_Covered_Drug_or_Biological2 from cms_2015 where NDC_of_Associated_Covered_Drug_or_Biological2 = "0169-4060-90"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic statistics on data
# MAGIC * Find number of empty values to clean the dataset
# MAGIC * Find cardinality of the column values

# COMMAND ----------

# Load the dataset 
df = spark.read.parquet("/mnt/mwc/cms_p/2015")
df.createOrReplaceTempView("cms_2015")
df.count()

# COMMAND ----------

from pyspark.sql.functions import *

col_names = df.columns
  
emptyValList = []
# Function that finds the number of empty values per column
def countEmptyValues(colName):
  # Find the number of empty values per record
  count = df.where(colName + "=''").count()
  emptyValList.append(count)

# Apply function on every column in stringColList
map(countEmptyValues, col_names)
zip(col_names, emptyValList)

# COMMAND ----------

tuples = zip(col_names, emptyValList)
sorted_tuples = sorted(tuples, key=lambda tup: tup[1], reverse=True)
sorted_tuples

# COMMAND ----------

# Cleaned existing data by reducing the number of columns in half by removing empty values 
filtered_cols = [x[0] for x in sorted_tuples if x[1] > 11132000/2] 
filtered_cols
for c in filtered_cols:
  df = df.drop(c)
len(df.columns)

# COMMAND ----------

df.createOrReplaceTempView("cms_2015")

# COMMAND ----------

# Create a List of Column Names with data type = string
stringColList = [i[0] for i in df.dtypes if i[1] == 'string']
[c for c in stringColList]

# COMMAND ----------

# Create a function that performs a countDistinct(colName) for string categories
distinctList = []
def countDistinctCats(colName):
  count = df.agg(countDistinct(colName)).collect()[0][0]
  distinctList.append(count)
  
map(countDistinctCats, stringColList)

# COMMAND ----------

distinct_vals = zip(stringColList, distinctList)
sorted_distinct = sorted(distinct_vals, key=lambda tup: tup[1], reverse=True)
sorted_distinct

# COMMAND ----------

def convert_human_readable(total):
  import locale
  locale.setlocale( locale.LC_ALL, '' )
  try:
    return locale.currency( total, grouping=True )
  except:
    return 0
  
def verify_numeric(amt):
  try:
    return float(amt)
  except:
    return 0

spark.udf.register("convert_human_readable", convert_human_readable)
spark.udf.register("verify_numeric", verify_numeric)

spark.sql("set spark.sql.shuffle.partitions=160")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC name_of_covered_drug_brand,
# MAGIC name_of_covered_drug,
# MAGIC total_spend,
# MAGIC total_spend_hr,
# MAGIC num_of_records,
# MAGIC avg_unit_price,
# MAGIC avg_unit_price_raw,
# MAGIC Physician_Profile_ID 
# MAGIC from (
# MAGIC   select 
# MAGIC     lower(Name_of_Associated_Covered_Drug_or_Biological1) as name_of_covered_drug_brand,
# MAGIC     lower(Name_of_Associated_Covered_Drug_or_Biological2) as name_of_covered_drug,
# MAGIC     sum(verify_numeric(Total_Amount_of_Payment_USDollars)) as total_spend, 
# MAGIC     convert_human_readable(sum(Total_Amount_of_Payment_USDollars)) as total_spend_hr,
# MAGIC     count(1) as num_of_records,
# MAGIC     convert_human_readable(sum(verify_numeric(Total_Amount_of_Payment_USDollars)) / count(1)) as avg_unit_price,
# MAGIC     sum(verify_numeric(Total_Amount_of_Payment_USDollars)) / count(1) as avg_unit_price_raw,
# MAGIC     Physician_Profile_ID 
# MAGIC   from cms_2015 
# MAGIC     where Name_of_Associated_Covered_Drug_or_Biological1 != "" 
# MAGIC     and Physician_Profile_ID != ""
# MAGIC     group by Physician_Profile_ID, 
# MAGIC       Name_of_Associated_Covered_Drug_or_Biological1,
# MAGIC       Name_of_Associated_Covered_Drug_or_Biological2
# MAGIC     order by total_spend desc
# MAGIC   )

# COMMAND ----------

df = spark.sql("""
select
name_of_covered_drug_brand,
name_of_covered_drug,
drug_id,
total_spend,
total_spend_hr,
num_of_records,
avg_unit_price,
avg_unit_price_raw,
Physician_Profile_ID 
from (
  select 
    lower(Name_of_Associated_Covered_Drug_or_Biological1) as name_of_covered_drug_brand,
    lower(Name_of_Associated_Covered_Drug_or_Biological2) as name_of_covered_drug,
    NDC_of_Associated_Covered_Drug_or_Biological1 as drug_id,
    sum(verify_numeric(Total_Amount_of_Payment_USDollars)) as total_spend, 
    convert_human_readable(sum(Total_Amount_of_Payment_USDollars)) as total_spend_hr,
    count(1) as num_of_records,
    convert_human_readable(sum(verify_numeric(Total_Amount_of_Payment_USDollars)) / count(1)) as avg_unit_price,
    sum(verify_numeric(Total_Amount_of_Payment_USDollars)) / count(1) as avg_unit_price_raw,
    Physician_Profile_ID 
  from cms_2015 
    where Name_of_Associated_Covered_Drug_or_Biological1 != "" 
    and Physician_Profile_ID != ""
    group by Physician_Profile_ID, 
      Name_of_Associated_Covered_Drug_or_Biological1,
      Name_of_Associated_Covered_Drug_or_Biological2,
      NDC_of_Associated_Covered_Drug_or_Biological1
    order by total_spend desc
  )
 """)
dbutils.fs.rm("/mnt/mwc/cms_2015_unit_price", True)
df.coalesce(40).write.parquet("/mnt/mwc/cms_2015_unit_price")

# COMMAND ----------

spark.read.parquet("/mnt/mwc/cms_2015_unit_price").createOrReplaceTempView("cms_2015_unit_price")
display(table("cms_2015_unit_price"))

# COMMAND ----------

ids = spark.sql("select distinct(drug_id) as d_id, avg(avg_unit_price_raw) as avg from cms_2015_unit_price group by drug_id order by avg desc limit 300").select("d_id").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Physician Dataset 
# MAGIC Source data link: https://openpaymentsdata.cms.gov/api/views/yfgg-mzqb/rows.json?accessType=DOWNLOAD
# MAGIC 
# MAGIC 
# MAGIC Possibly get this dataset?
# MAGIC https://data.medicare.gov/Physician-Compare/Physician-Compare-National-Downloadable-File/mj5m-pzi6
# MAGIC https://data.medicare.gov/api/views/mj5m-pzi6/rows.json?accessType=DOWNLOAD

# COMMAND ----------

# MAGIC %run "/Users/mwc@databricks.com/Medicare Analysis/_etl_helper.py"

# COMMAND ----------

fetch_and_create_table("https://openpaymentsdata.cms.gov/api/views/yfgg-mzqb/rows.json?accessType=DOWNLOAD", "physician_tbl")

# COMMAND ----------

df = spark.read.parquet("/mnt/mwc/physician_tbl")
df.createOrReplaceTempView("physician_tbl")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.shuffle.partitions=150

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(a.physician_profile_id))
# MAGIC   from physician_tbl a 
# MAGIC   join cms_2015 b 
# MAGIC   on a.physician_profile_id = b.Physician_Profile_ID 
# MAGIC   where a.physician_profile_id != ""

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 11M payment records
# MAGIC select count(*) as num_records from cms_2015

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(Total_Amount_of_Payment_USDollars) as total_spend, 
# MAGIC   Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State, 
# MAGIC   Recipient_State,
# MAGIC   Physician_Primary_Type 
# MAGIC from cms_2015 
# MAGIC where Physician_Primary_Type != ""
# MAGIC group by 
# MAGIC Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State, 
# MAGIC Recipient_State,
# MAGIC Physician_Primary_Type
# MAGIC order by total_spend desc 
# MAGIC limit 50

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) as counts, Name_of_Associated_Covered_Device_or_Medical_Supply1 
# MAGIC from cms_2015 
# MAGIC where Name_of_Associated_Covered_Device_or_Medical_Supply1 != ""
# MAGIC group by Name_of_Associated_Covered_Device_or_Medical_Supply1 
# MAGIC order by counts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 616k unique physicians
# MAGIC select count(distinct(Physician_Profile_ID)) from cms_2015

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) as counts, Physician_Profile_ID from cms_2015 where Physician_Profile_ID != "" group by Physician_Profile_ID order by counts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cms_2015 where Physician_Profile_ID = 362632

# COMMAND ----------

# MAGIC %md
# MAGIC Units are jointly funded; the Federal government currently reimburses each of the States
# MAGIC 75 percent of the costs of operating a Unit, and the States contribute the remaining 25 percent.4
# MAGIC In FY 2014, combined Federal and State expenditures for the Units totaled $235 million
# MAGIC https://oig.hhs.gov/oei/reports/oei-06-15-00010.pdf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Try to run UDFs / queries to process and identify these claims programmatically.
# MAGIC 
# MAGIC Home health care aides: 30 percent of criminal convictions
# MAGIC In FY 2014, criminal convictions of home health care aides represented 30 percent of all MFCU
# MAGIC criminal convictions, a small increase from the FY 2013 figure of 26 percent. Most commonly,
# MAGIC home health care aides were convicted of fraud, often for claiming to have rendered services that
# MAGIC were not provided. For example, the Nebraska MFCU investigated several home health care
# MAGIC aides who submitted timesheets for services that were never rendered or timesheets that claimed
# MAGIC that the aide was working with more than one patient at the same time in different locations.
# MAGIC Two of these Nebraska home health care aides were convicted; one was sentenced to 18 months
# MAGIC of probation and ordered to pay restitution, and the other was sentenced to 2 years of probation
# MAGIC and ordered to perform 200 hours of community service.
# MAGIC Certified nursing aides: 9 percent of criminal convictions
# MAGIC In FY 2014, criminal convictions of certified nursing aides represented 9 percent of all MFCU
# MAGIC criminal convictions, a slight increase from the FY 2013 figure of 8 percent. These convictions
# MAGIC involved offenses such as patient abuse, billing for services not rendered, and falsifying
# MAGIC timesheets. For example, the New York MFCU investigated a certified nursing aide who falsely
# MAGIC documented provision of services. This aide was sentenced to 15 days in jail, fined $125, and
# MAGIC required to surrender her certified nursing aide certificate. 

# COMMAND ----------

