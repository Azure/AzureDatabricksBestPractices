# Databricks notebook source
# DBTITLE 1,Configuration Variables
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "20046851-fa30-4349-8c8b-f7a0a0460151",
           "dfs.adls.oauth2.credential": "cZocfFwXpGmfpi24kP+tvGBldsi/xPkErU9QHL4jwfw=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/be413eec-6262-4083-97c8-8c2a817c2fe1/oauth2/token"}

adls_base = "dbfs:/mnt/ndwpocdl/"
incoming_dir = "incoming/"

# COMMAND ----------

# DBTITLE 1,(Optional) Unmount Azure Data Lake
dbutils.fs.unmount("/mnt/ndwpocdl")

# COMMAND ----------

# DBTITLE 1,Mount Azure Data Lake
dbutils.fs.mount(
  source = "adl://ndwpocdl.azuredatalakestore.net",
  mount_point = "/mnt/ndwpocdl",
  extra_configs = configs
)

# COMMAND ----------

# MAGIC %fs ls /mnt/ndwpocdl/

# COMMAND ----------

x = dbutils.fs.ls("/mnt/ndwpocdl/")
keep = ["incoming", "archive", "failed", "serving", "processing", "parquet_files"]

for fi in x:
  if (fi.name not in keep):
    print("DEL:", fi.path)
  else:
    print("KEEP:", fi.path)

# COMMAND ----------

keep = ["incoming", "archive", "failed", "serving", "processing", "parquet_files", "dbfs:/mnt/ndwpocdl/parquet_files/"]

for fi in x:
  if ("site_cd" in fi.path):
    print("DELETING", fi.path)
    dbutils.fs.rm(fi.path, True)

# COMMAND ----------

y = dbutils.fs.ls("/mnt/ndwpocdl/")
for f in y:
  print(f.path)

# COMMAND ----------

# DBTITLE 1,Begin Fact Alarm Import
fact_alarm_dl = adls_base+incoming_dir+"fact_alarm/fact_alarm.txt"

fact_alarm_df = (spark.read
        .option("delimiter", ",") #This is how we could pass in a Tab or other delimiter.
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",") #sep, quote, escape are needed for escaping the ',' char in cells
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "\N")
        .csv(fact_alarm_dl)
).cache().createOrReplaceTempView("fact_alarm_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE fact_alarm 
# MAGIC SELECT * FROM fact_alarm_raw;
# MAGIC 
# MAGIC SELECT * FROM fact_alarm

# COMMAND ----------

#display(fact_alarm_df)
#fact_alarm_df.select("_c22").distinct().show()

# COMMAND ----------

# DBTITLE 1,Begin Cell Info Import
v_dwh_association_cell_info_dl = adls_base+incoming_dir+"v_dwh_association_cell_info/dwh_association_cell_info.txt"

v_dwh_association_cell_info_df = (spark.read
        .option("delimiter", ",") #This is how we could pass in a Tab or other delimiter.
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",") #sep, quote, escape are needed for escaping the ',' char in cells
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "\N")
        .csv(v_dwh_association_cell_info_dl)
).cache().createOrReplaceTempView("v_dwh_association_cell_info_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE v_dwh_association_cell_info 
# MAGIC SELECT * FROM v_dwh_association_cell_info_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT sector_status_desc FROM v_dwh_association_cell_info

# COMMAND ----------

#display(spark.sql("select * from v_dwh_association_cell_info"))

# COMMAND ----------

#display(v_dwh_association_cell_info_df)
#v_dwh_association_cell_info_df.select("nortel_site_cd").distinct().show()

# COMMAND ----------

# DBTITLE 1,Begin Site Sector Import
ref_dwh_cgi_site_sector_lkp_dl = adls_base+incoming_dir+"ref_dwh_cgi_site_sector_lkp/ref_dwh_cgi_site_sector_lkp.txt"

ref_dwh_cgi_site_sector_lkp_df = (spark.read
        .option("delimiter", ",") #This is how we could pass in a Tab or other delimiter.
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",") #sep, quote, escape are needed for escaping the ',' char in cells
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "\N")
        .csv(ref_dwh_cgi_site_sector_lkp_dl)
).cache().createOrReplaceTempView("ref_dwh_cgi_site_sector_lkp_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE ref_dwh_cgi_site_sector_lkp 
# MAGIC SELECT * FROM ref_dwh_cgi_site_sector_lkp_raw;

# COMMAND ----------

#display(ref_dwh_cgi_site_sector_lkp_df)
#ref_dwh_cgi_site_sector_lkp_df.select("ci_mkt_cd").distinct().show()

# COMMAND ----------

# DBTITLE 1,Begin Site Mkt Import
lkp_site_mkt_dl = adls_base+incoming_dir+"lkp_site_mkt/lkp_site_mkt.txt"

lkp_site_mkt_df = (spark.read
        .option("delimiter", ",") #This is how we could pass in a Tab or other delimiter.
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",") #sep, quote, escape are needed for escaping the ',' char in cells
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "\N")
        .csv(lkp_site_mkt_dl)
).cache().createOrReplaceTempView("lkp_site_mkt_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE lkp_site_mkt 
# MAGIC SELECT * FROM lkp_site_mkt_raw

# COMMAND ----------

#display(lkp_site_market_df)
#lkp_site_market_df.select("region_name").distinct().show()

# COMMAND ----------

# DBTITLE 1,Begin Fact Site Down Import
fact_site_down_dl = adls_base+incoming_dir+"fact_site_down/fact_site_down.txt"

fact_site_down_df = (spark.read
        .option("delimiter", ",") #This is how we could pass in a Tab or other delimiter.
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",") #sep, quote, escape are needed for escaping the ',' char in cells
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "\N")
        .csv(fact_site_down_dl)
).cache().createOrReplaceTempView("fact_site_down_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE fact_site_down 
# MAGIC SELECT * FROM fact_site_down_raw

# COMMAND ----------

#display(fact_site_down_df)
#fact_site_down_df.select("gsm_nest_override_flg").distinct().show()

# COMMAND ----------

# DBTITLE 1,Begin Dim Sector Import
dim_sector_dl = adls_base+incoming_dir+"dim_sector/dim_sector.txt"

dim_sector_df = (spark.read
        .option("delimiter", ",") #This is how we could pass in a Tab or other delimiter.
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",") #sep, quote, escape are needed for escaping the ',' char in cells
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "\N")
        .csv(dim_sector_dl)
).cache().createOrReplaceTempView("dim_sector_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE dim_sector 
# MAGIC SELECT * FROM dim_sector_raw

# COMMAND ----------

#display(dim_sector_df)
#dim_sector_df.select("sector_status").distinct().show()

# COMMAND ----------

# DBTITLE 1,Begin Dim Site Import
dim_site_dl = adls_base+incoming_dir+"dim_site/dim_site.txt"

dim_site_df = (spark.read
        .option("delimiter", ",") #This is how we could pass in a Tab or other delimiter.
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",") #sep, quote, escape are needed for escaping the ',' char in cells
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "\N")
        .csv(dim_site_dl)
).cache().createOrReplaceTempView("dim_site_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE dim_site 
# MAGIC SELECT * FROM dim_site_raw

# COMMAND ----------

#display(dim_site_df)
#dim_site_df.select("gsm_nest_override_flg").distinct().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM REF_DWH_CGI_SITE_SECTOR_LKP

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM REF_DWH_CGI_SITE_SECTOR_LKP
# MAGIC WHERE CAST(left(sector_cd,1) as INT) is null

# COMMAND ----------

# MAGIC %sql
# MAGIC --INSERT OVERWRITE TABLE association_oss_cell_info
# MAGIC SELECT
# MAGIC 0 as oss_cell_info_id,
# MAGIC sr.site_cd, -- look up REF_DWH_CGI_SITE_SECTOR_LKP on OSS_CGI, NETWORK_HIERARCHY & OSS_VENDOR and populate (8 character)
# MAGIC sr.sector_cd, -- look up REF_DWH_CGI_SITE_SECTOR_LKP on OSS_CGI, NETWORK_HIERARCHY & OSS_VENDOR and populate (A0GPA, 11UAA, 21LAA)
# MAGIC case
# MAGIC when CAST(left(sr.sector_cd,1) as INT) is null then "GSM"
# MAGIC when CAST(left(sr.sector_cd,1) as INT) is not null and sr.sector_cd not like '%L%' THEN "UMTS"
# MAGIC when CAST(left(sr.sector_cd,1) as INT) IS NOT NULL and sr.sector_cd like '%L%' THEN "LTE"
# MAGIC else NULL
# MAGIC end as sector_technology_desc, -- decode using code below
# MAGIC v.oss_vendor -- key column
# MAGIC ,v.network_hierarchy -- key column
# MAGIC         ,v.oss_cgi -- key column
# MAGIC      ,v.source_oss_server_desc
# MAGIC      ,v.oss_site_name
# MAGIC      ,v.oss_sector_name
# MAGIC      ,v.oss_cell_name
# MAGIC      ,oss_parent_equip_cd
# MAGIC      ,parent_equip_cd
# MAGIC      ,parent_equip_type_desc
# MAGIC      ,oss_mcc
# MAGIC      ,oss_mnc
# MAGIC      ,oss_lac
# MAGIC      ,oss_cell_id
# MAGIC      ,cell_operational_state_desc
# MAGIC      ,cell_administration_state_desc
# MAGIC      ,cell_barred_state_desc
# MAGIC      ,sector_status_desc
# MAGIC      ,sector_status_rank_preference
# MAGIC      ,oss_parent_obj_cd
# MAGIC      ,oss_parent_obj_type_desc
# MAGIC      ,oss_site_equip_cd
# MAGIC      ,oss_site_equip_type_desc
# MAGIC      ,oss_parent_equip_mkt_cd
# MAGIC      ,oss_mkt_name
# MAGIC      ,reason_desc
# MAGIC      ,alarm_text
# MAGIC      ,last_sync_dt
# MAGIC      ,test_cell_flg
# MAGIC      ,source_created_by_id
# MAGIC      ,source_created_dt
# MAGIC      ,source_modified_by_id
# MAGIC      ,source_modified_dt
# MAGIC      ,nortel_site_cd
# MAGIC      ,oss_enodeb_id
# MAGIC      ,oss_tac
# MAGIC      ,oss_pci
# MAGIC      ,oss_ecgi
# MAGIC      ,node_type_desc
# MAGIC  FROM V_DWH_ASSOCIATION_CELL_INFO v
# MAGIC  LEFT JOIN REF_DWH_CGI_SITE_SECTOR_LKP sr on (v.OSS_CGI = sr.OSS_CGI) and (v.NETWORK_HIERARCHY = sr.NETWORK_HIERARCHY) and (v.OSS_VENDOR = sr.OSS_VENDOR) WHERE sector_technology_desc = "GSM"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED association_oss_cell_info
# MAGIC --SELECT COUNT(*) FROM association_oss_cell_info

# COMMAND ----------

# MAGIC %fs ls 	dbfs:/mnt/ndwpocdl/serving/ASSOCIATION_OSS_CELL_INFO_parquet