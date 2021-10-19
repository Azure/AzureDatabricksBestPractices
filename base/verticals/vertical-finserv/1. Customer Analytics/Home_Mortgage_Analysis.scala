// Databricks notebook source
display(dbutils.fs.ls("/mnt/wesley/homemortgage"))

// COMMAND ----------

val filePath = "dbfs:/mnt/wesley/homemortgage/hmda_lar.csv"
println("Number of Rows: " + sc.textFile(filePath).count())

// COMMAND ----------

val fileSize = dbutils.fs.ls("dbfs:/mnt/wesley/homemortgage/hmda_lar.csv").map(_.size).sum / (Math.pow(1024.0,3))
println(f"Size of file in GB: $fileSize%2.2f")

// COMMAND ----------

val rawAuth = sqlContext.read.format("csv").option("delimiter", ",").option("header","true").option("inferSchema", "true").load("dbfs:/mnt/wesley/homemortgage/hmda_lar.csv")

rawAuth.toDF("tract_to_msamd_income",
"rate_spread",
"population",
"minority_population",
"number_of_owner_occupied_units",
"number_of_1_to_4_family_units",
"loan_amount_000s",
"hud_median_family_income",
"applicant_income_000s",
"state_name",
"state_abbr",
"sequence_number",
"respondent_id",
"purchaser_type_name",
"property_type_name",
"preapproval_name",
"owner_occupancy_name",
"msamd_name",
"loan_type_name",
"loan_purpose_name",
"lien_status_name",
"hoepa_status_name",
"edit_status_name",
"denial_reason_name_3",
"denial_reason_name_2",
"denial_reason_name_1",
"county_name",
"co_applicant_sex_name",
"co_applicant_race_name_5",
"co_applicant_race_name_4",
"co_applicant_race_name_3",
"co_applicant_race_name_2",
"co_applicant_race_name_1",
"co_applicant_ethnicity_name",
"census_tract_number",
"as_of_year",
"application_date_indicator",
"applicant_sex_name",
"applicant_race_name_5",
"applicant_race_name_4",
"applicant_race_name_3",
"applicant_race_name_2",
"applicant_race_name_1",
"applicant_ethnicity_name",
"agency_name",
"agency_abbr",
"action_taken_name").repartition((fileSize/0.25).toInt).write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("hmda_lar")


// COMMAND ----------

// DBTITLE 1,Step2: Explore Home Mortgage Data 
// MAGIC %md Print the schema, display the first 1,000 rows, use describe to get some statistics

// COMMAND ----------

// DBTITLE 1,Run SQL Query on Table
// MAGIC %sql SELECT * FROM hmda_lar

// COMMAND ----------

// DBTITLE 1,Describe Statistics of Table
display(table("hmda_lar").describe())

// COMMAND ----------

// DBTITLE 1,Prepare the data
import sqlContext._

val df = sql("select state_name,state_abbr,county_name,owner_occupancy_name,loan_amount_000s,loan_purpose_name,loan_type_name, agency_abbr,respondent_id,population,applicant_income_000s,msamd_name, loan_amount_000s/applicant_income_000s as ratio, case when loan_type_name = 'Conventional' then 'Conventional' else 'Nonconventional' end conv, case when state_abbr in ('CA','TX','FL','NY','IL','CO','GA','NC','OH','WA','MI', 'VA') then state_abbr else 'other' end st2 from hmda_lar")

df.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("hmda_lar_clean")

// COMMAND ----------

// DBTITLE 1,Examine data
// MAGIC %sql select * from hmda_lar_clean

// COMMAND ----------

// MAGIC %md
// MAGIC ###Step 3: Visualization
// MAGIC * Show the distribution of the account length.

// COMMAND ----------

// MAGIC %sql  select loan_purpose_name,count(*) as total_loans from hmda_lar_clean group by loan_purpose_name order by total_loans desc

// COMMAND ----------

// MAGIC %md 
// MAGIC In 2016 HMDA data there were about 7.6 million first-lien home purchase and refinance loans on 1-4 unit properties. NOTE: these numbers are not adjusted for coverage. HMDA does not cover all of the U.S. mortgage market, but the coverage is likely quite high (90% or more) in recent years.
// MAGIC 
// MAGIC About 3.9 million were home purchase and about 3.7 million were refinance loans. In the U.S. mortgage market it’s common to distinguish between conventional and nonconventional loans. Nonconventional loans are loans insured by the Federal Housing Administration (FHA), guaranteed by the Department of Veterans Affairs (VA) and loans backed by the Farm Service Agency (FSA) or Rural Housing Service (RHS).

// COMMAND ----------

// MAGIC %sql  select a.loan_purpose_name,a.conv,count(*) as total_loans from hmda_lar_clean a group by a.loan_purpose_name,a.conv order by total_loans desc

// COMMAND ----------

// MAGIC %md We see here that while the total mortgage market had slightly more home purchase than refinance loans, conventional mortgages were skewed toward refiance loans.

// COMMAND ----------

// MAGIC %sql  select a.loan_purpose_name,a.state_abbr,count(*) as total_loans from hmda_lar_clean a group by a.loan_purpose_name,a.state_abbr order by total_loans desc

// COMMAND ----------

// MAGIC %sql  select a.loan_purpose_name,a.state_abbr,count(*) as total_loans from hmda_lar_clean a group by a.loan_purpose_name,a.state_abbr order by total_loans desc limit 90

// COMMAND ----------

// MAGIC %md The U.S. mortgage market is dominated by several large states, California in particular. 

// COMMAND ----------

// MAGIC %sql  select a.state_abbr,sum(a.loan_amount_000s) as total_loans from hmda_lar_clean a group by a.state_abbr