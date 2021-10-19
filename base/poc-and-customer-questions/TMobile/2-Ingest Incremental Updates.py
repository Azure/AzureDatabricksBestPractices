# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Store base file paths

# COMMAND ----------

adls_base = "dbfs:/mnt/ndwpocdl/"
incoming_dir = "incoming/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Explicitly define the schema for `v_dwh_association_cell_info`

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType

v_dwh_association_cell_info_schema = (StructType([
  StructField('oss_vendor',StringType(), True),
  StructField('sector_technology_desc',StringType(), True),
  StructField('source_oss_server_desc',StringType(), True),
  StructField('network_hierarchy',StringType(), True),
  StructField('oss_site_name',StringType(), True),
  StructField('oss_sector_name',StringType(), True),
  StructField('oss_cell_name',StringType(), True),
  StructField('oss_parent_equip_cd',StringType(), True),
  StructField('parent_equip_cd',StringType(), True),
  StructField('parent_equip_type_desc',StringType(), True),
  StructField('oss_cgi',StringType(), True),
  StructField('oss_mcc',StringType(), True),
  StructField('oss_mnc',StringType(), True),
  StructField('oss_lac',StringType(), True),
  StructField('oss_cell_id',StringType(), True),
  StructField('cell_operational_state_desc',StringType(), True),
  StructField('cell_administration_state_desc',StringType(), True),
  StructField('cell_barred_state_desc',StringType(), True),
  StructField('sector_status_desc',StringType(), True),
  StructField('sector_status_rank_preference',StringType(), True),
  StructField('oss_parent_obj_cd',StringType(), True),
  StructField('oss_parent_obj_type_desc',StringType(), True),
  StructField('oss_site_equip_cd',StringType(), True),
  StructField('oss_site_equip_type_desc',StringType(), True),
  StructField('oss_parent_equip_mkt_cd',StringType(), True),
  StructField('oss_mkt_name',StringType(), True),
  StructField('reason_desc',StringType(), True),
  StructField('alarm_text',StringType(), True),
  StructField('last_sync_dt',TimestampType(), True),
  StructField('test_cell_flg',StringType(), True),
  StructField('source_created_by_id',StringType(), True),
  StructField('source_created_dt',TimestampType(), True),
  StructField('source_modified_by_id',StringType(), True),
  StructField('source_modified_dt',TimestampType(), True),
  StructField('nortel_site_cd',StringType(), True),
  StructField('oss_enodeb_id',StringType(), True),
  StructField('oss_tac',StringType(), True),
  StructField('oss_pci',StringType(), True),
  StructField('oss_ecgi',StringType(), True),
  StructField('node_type_desc',StringType(), True),
  StructField('ct_SYS_CHANGE_VERSION',StringType(), True),
  StructField('ct_SYS_CHANGE_CREATION_VERSION',StringType(), True),
  StructField('ct_SYS_CHANGE_OPERATION',StringType(), True),
  StructField('ct_SYS_CHANGE_COLUMNS',StringType(), True),
  StructField('ct_SYS_CHANGE_CONTEXT',StringType(), True),
  StructField('ct_oss_cgi',StringType(), True),
  StructField('ct_oss_vendor',StringType(), True),
  StructField('ct_network_hierarchy',StringType(), True)
]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Import incremental `v_dwh_association_cell_info` file and create a temporary view

# COMMAND ----------

# DBTITLE 0,Begin Cell Info Import
v_dwh_association_cell_info_dl = adls_base+incoming_dir+"incremental_v_dwh_association_cell_info/dwh_association_cell_info.txt"

v_dwh_association_cell_info_df = (spark.read
        .option("delimiter", ",") #This is how we could pass in a Tab or other delimiter.
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", ",") #sep, quote, escape are needed for escaping the ',' char in cells
        .option("quote", '"')
        .option("escape", '"')
        .option("nullValue", "\N")
        .schema(v_dwh_association_cell_info_schema)                          
        .csv(v_dwh_association_cell_info_dl)
).cache().createOrReplaceTempView("v_dwh_association_cell_info_incremental")

#display(v_dwh_association_cell_info_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_dwh_association_cell_info_incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE v_dwh_association_cell_info_incremental

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Determine deleted records

# COMMAND ----------

df_changed_deletes = spark.sql("""
                      SELECT ct_oss_cgi AS d_oss_cgi, ct_network_hierarchy AS d_network_hierarchy, ct_oss_vendor AS d_oss_vendor
                      FROM v_dwh_association_cell_info_incremental
                      WHERE ct_SYS_CHANGE_OPERATION = 'D'
                      """).cache().createOrReplaceTempView("deleted_keys")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), ct_SYS_CHANGE_OPERATION FROM v_dwh_association_cell_info_incremental
# MAGIC GROUP BY ct_SYS_CHANGE_OPERATION 

# COMMAND ----------

df_changed_updates = spark.sql("""
                      SELECT *
                      FROM v_dwh_association_cell_info_incremental
                      WHERE ct_SYS_CHANGE_OPERATION <> 'D'
                      """).cache().createOrReplaceTempView("updated_and_inserted_rows")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Handle delete case of `V_DWH_ASSOCIATION_CELL_INFO` table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM V_DWH_ASSOCIATION_CELL_INFO v
# MAGIC WHERE EXISTS (
# MAGIC   SELECT d_oss_vendor, d_oss_cgi, d_network_hierarchy
# MAGIC   FROM deleted_keys
# MAGIC   WHERE v.oss_vendor = d_oss_vendor AND
# MAGIC         v.oss_cgi = d_oss_cgi AND
# MAGIC         v.network_hierarchy = d_network_hierarchy
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Merge updates into existing `V_DWH_ASSOCIATION_CELL_INFO` table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC set spark.sql.shuffle.partitions=16;
# MAGIC 
# MAGIC MERGE INTO  V_DWH_ASSOCIATION_CELL_INFO t
# MAGIC USING  updated_and_inserted_rows inc
# MAGIC ON  inc.ct_oss_vendor = t.oss_vendor and 
# MAGIC     inc.ct_oss_cgi = t.oss_cgi and 
# MAGIC     inc.ct_network_hierarchy = t.network_hierarchy
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC 	t.oss_vendor=inc.oss_vendor ,
# MAGIC 	t.sector_technology_desc=inc.sector_technology_desc ,
# MAGIC 	t.source_oss_server_desc=inc.source_oss_server_desc ,
# MAGIC 	t.network_hierarchy=inc.network_hierarchy ,
# MAGIC 	t.oss_site_name=inc.oss_site_name ,
# MAGIC 	t.oss_sector_name=inc.oss_sector_name ,
# MAGIC 	t.oss_cell_name=inc.oss_cell_name ,
# MAGIC 	t.oss_parent_equip_cd=inc.oss_parent_equip_cd ,
# MAGIC 	t.parent_equip_cd=inc.parent_equip_cd ,
# MAGIC 	t.parent_equip_type_desc=inc.parent_equip_type_desc ,
# MAGIC 	t.oss_cgi=inc.oss_cgi ,
# MAGIC 	t.oss_mcc=inc.oss_mcc ,
# MAGIC 	t.oss_mnc=inc.oss_mnc ,
# MAGIC 	t.oss_lac=inc.oss_lac ,
# MAGIC 	t.oss_cell_id=inc.oss_cell_id ,
# MAGIC 	t.cell_operational_state_desc=inc.cell_operational_state_desc ,
# MAGIC 	t.cell_administration_state_desc=inc.cell_administration_state_desc ,
# MAGIC 	t.cell_barred_state_desc=inc.cell_barred_state_desc ,
# MAGIC 	t.sector_status_desc=inc.sector_status_desc ,
# MAGIC 	t.sector_status_rank_preference=inc.sector_status_rank_preference ,
# MAGIC 	t.oss_parent_obj_cd=inc.oss_parent_obj_cd ,
# MAGIC 	t.oss_parent_obj_type_desc=inc.oss_parent_obj_type_desc ,
# MAGIC 	t.oss_site_equip_cd=inc.oss_site_equip_cd ,
# MAGIC 	t.oss_site_equip_type_desc=inc.oss_site_equip_type_desc ,
# MAGIC 	t.oss_parent_equip_mkt_cd=inc.oss_parent_equip_mkt_cd ,
# MAGIC 	t.oss_mkt_name=inc.oss_mkt_name ,
# MAGIC 	t.reason_desc=inc.reason_desc ,
# MAGIC 	t.alarm_text=inc.alarm_text ,
# MAGIC 	t.last_sync_dt=inc.last_sync_dt  ,
# MAGIC 	t.test_cell_flg=inc.test_cell_flg ,
# MAGIC 	t.source_created_by_id=inc.source_created_by_id ,
# MAGIC 	t.source_created_dt=inc.source_created_dt  ,
# MAGIC 	t.source_modified_by_id=inc.source_modified_by_id ,
# MAGIC 	t.source_modified_dt=inc.source_modified_dt  ,
# MAGIC 	t.nortel_site_cd=inc.nortel_site_cd ,
# MAGIC 	t.oss_enodeb_id=inc.oss_enodeb_id ,
# MAGIC 	t.oss_tac=inc.oss_tac ,
# MAGIC 	t.oss_pci=inc.oss_pci ,
# MAGIC 	t.oss_ecgi=inc.oss_ecgi ,
# MAGIC 	t.node_type_desc=inc.node_type_desc 
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (oss_vendor , sector_technology_desc , source_oss_server_desc ,
# MAGIC 	network_hierarchy , oss_site_name , oss_sector_name , oss_cell_name ,
# MAGIC 	oss_parent_equip_cd , parent_equip_cd , parent_equip_type_desc ,
# MAGIC 	oss_cgi , oss_mcc , oss_mnc ,
# MAGIC 	oss_lac , oss_cell_id , cell_operational_state_desc ,
# MAGIC 	cell_administration_state_desc , cell_barred_state_desc ,
# MAGIC 	sector_status_desc , sector_status_rank_preference ,
# MAGIC 	oss_parent_obj_cd , oss_parent_obj_type_desc ,
# MAGIC 	oss_site_equip_cd , oss_site_equip_type_desc ,
# MAGIC 	oss_parent_equip_mkt_cd , oss_mkt_name ,
# MAGIC 	reason_desc , alarm_text ,
# MAGIC 	last_sync_dt , test_cell_flg , source_created_by_id ,
# MAGIC 	source_created_dt , source_modified_by_id , source_modified_dt ,
# MAGIC 	nortel_site_cd , oss_enodeb_id , oss_tac ,
# MAGIC 	oss_pci , oss_ecgi , node_type_desc )
# MAGIC   VALUES(inc.oss_vendor , inc.sector_technology_desc , inc.source_oss_server_desc ,
# MAGIC 	inc.network_hierarchy , inc.oss_site_name , inc.oss_sector_name , inc.oss_cell_name ,
# MAGIC 	inc.oss_parent_equip_cd , inc.parent_equip_cd , inc.parent_equip_type_desc ,
# MAGIC 	inc.oss_cgi , inc.oss_mcc , inc.oss_mnc ,
# MAGIC 	inc.oss_lac , inc.oss_cell_id , inc.cell_operational_state_desc ,
# MAGIC 	inc.cell_administration_state_desc , inc.cell_barred_state_desc ,
# MAGIC 	inc.sector_status_desc , inc.sector_status_rank_preference ,
# MAGIC 	inc.oss_parent_obj_cd , inc.oss_parent_obj_type_desc ,
# MAGIC 	inc.oss_site_equip_cd , inc.oss_site_equip_type_desc ,
# MAGIC 	inc.oss_parent_equip_mkt_cd , inc.oss_mkt_name ,
# MAGIC 	inc.reason_desc , inc.alarm_text ,
# MAGIC 	inc.last_sync_dt , inc.test_cell_flg , inc.source_created_by_id ,
# MAGIC 	inc.source_created_dt , inc.source_modified_by_id , inc.source_modified_dt ,
# MAGIC 	inc.nortel_site_cd , inc.oss_enodeb_id , inc.oss_tac ,
# MAGIC 	inc.oss_pci , inc.oss_ecgi , inc.node_type_desc )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC BEGIN OVERWRITE CASE : 40.88sec

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE association_oss_cell_info
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
# MAGIC  LEFT JOIN REF_DWH_CGI_SITE_SECTOR_LKP sr on (v.OSS_CGI = sr.OSS_CGI) and (v.NETWORK_HIERARCHY = sr.NETWORK_HIERARCHY) and (v.OSS_VENDOR = sr.OSS_VENDOR)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC BEGIN DELETE, THEN UPDATE CASE: 45.21sec (4sec (DELETE) + 41.21sec (MERGE/INSERT))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DELETE FROM ASSOCIATION_OSS_CELL_INFO v
# MAGIC WHERE EXISTS (
# MAGIC   SELECT d_oss_vendor, d_oss_cgi, d_network_hierarchy
# MAGIC   FROM deleted_keys
# MAGIC   WHERE v.oss_vendor = d_oss_vendor AND
# MAGIC         v.oss_cgi = d_oss_cgi AND
# MAGIC         v.network_hierarchy = d_network_hierarchy
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW association_oss_cell_info_changes AS
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
# MAGIC  FROM updated_and_inserted_rows v
# MAGIC  LEFT JOIN REF_DWH_CGI_SITE_SECTOR_LKP sr on (v.OSS_CGI = sr.OSS_CGI) and (v.NETWORK_HIERARCHY = sr.NETWORK_HIERARCHY) and (v.OSS_VENDOR = sr.OSS_VENDOR)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM association_oss_cell_info_changes

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --set spark.sql.shuffle.partitions=16;
# MAGIC 
# MAGIC MERGE INTO  association_oss_cell_info t
# MAGIC USING  association_oss_cell_info_changes inc
# MAGIC ON  inc.oss_vendor = t.oss_vendor and 
# MAGIC     inc.oss_cgi = t.oss_cgi and 
# MAGIC     inc.network_hierarchy = t.network_hierarchy 
# MAGIC     --and inc.ct_SYS_CHANGE_OPERATION = 'U'
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC     t.oss_cell_info_id=inc.oss_cell_info_id ,
# MAGIC     t.site_cd=inc.site_cd ,
# MAGIC     t.sector_cd=inc.sector_cd ,
# MAGIC     t.sector_technology_desc=inc.sector_technology_desc ,
# MAGIC     t.oss_vendor=inc.oss_vendor ,
# MAGIC     t.source_oss_server_desc=inc.source_oss_server_desc ,
# MAGIC     t.network_hierarchy=inc.network_hierarchy ,
# MAGIC     t.oss_site_name=inc.oss_site_name ,
# MAGIC     t.oss_sector_name=inc.oss_sector_name ,
# MAGIC     t.oss_cell_name=inc.oss_cell_name ,
# MAGIC     t.oss_parent_equip_cd=inc.oss_parent_equip_cd ,
# MAGIC     t.parent_equip_cd=inc.parent_equip_cd ,
# MAGIC     t.parent_equip_type_desc=inc.parent_equip_type_desc ,
# MAGIC     t.oss_cgi=inc.oss_cgi ,
# MAGIC     t.oss_mcc=inc.oss_mcc ,
# MAGIC     t.oss_mnc=inc.oss_mnc ,
# MAGIC     t.oss_lac=inc.oss_lac ,
# MAGIC     t.oss_cell_id=inc.oss_cell_id ,
# MAGIC     t.cell_operational_state_desc=inc.cell_operational_state_desc ,
# MAGIC     t.cell_administration_state_desc=inc.cell_administration_state_desc ,
# MAGIC     t.cell_barred_state_desc=inc.cell_barred_state_desc ,
# MAGIC     t.sector_status_desc=inc.sector_status_desc ,
# MAGIC     t.sector_status_rank_preference=inc.sector_status_rank_preference ,
# MAGIC     t.oss_parent_obj_cd=inc.oss_parent_obj_cd ,
# MAGIC     t.oss_parent_obj_type_desc=inc.oss_parent_obj_type_desc ,
# MAGIC     t.oss_site_equip_cd=inc.oss_site_equip_cd ,
# MAGIC     t.oss_site_equip_type_desc=inc.oss_site_equip_type_desc ,
# MAGIC     t.oss_parent_equip_mkt_cd=inc.oss_parent_equip_mkt_cd ,
# MAGIC     t.oss_mkt_name=inc.oss_mkt_name ,
# MAGIC     t.reason_desc=inc.reason_desc ,
# MAGIC     t.alarm_text=inc.alarm_text ,
# MAGIC     t.last_sync_dt=inc.last_sync_dt ,
# MAGIC     t.test_cell_flg=inc.test_cell_flg ,
# MAGIC     t.source_created_by_id=inc.source_created_by_id ,
# MAGIC     t.source_created_dt=inc.source_created_dt ,
# MAGIC     t.source_modified_by_id=inc.source_modified_by_id ,
# MAGIC     t.source_modified_dt=inc.source_modified_dt ,
# MAGIC     t.nortel_site_cd=inc.nortel_site_cd ,
# MAGIC     t.oss_enodeb_id=inc.oss_enodeb_id ,
# MAGIC     t.oss_tac=inc.oss_tac ,
# MAGIC     t.oss_pci=inc.oss_pci ,
# MAGIC     t.oss_ecgi=inc.oss_ecgi ,
# MAGIC     t.node_type_desc=inc.node_type_desc
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT (
# MAGIC    oss_cell_info_id  ,
# MAGIC    site_cd  ,
# MAGIC    sector_cd  ,
# MAGIC    sector_technology_desc  ,
# MAGIC    oss_vendor  ,
# MAGIC    source_oss_server_desc  ,
# MAGIC    network_hierarchy  ,
# MAGIC    oss_site_name  ,
# MAGIC    oss_sector_name  ,
# MAGIC    oss_cell_name  ,
# MAGIC    oss_parent_equip_cd  ,
# MAGIC    parent_equip_cd  ,
# MAGIC    parent_equip_type_desc  ,
# MAGIC    oss_cgi  ,
# MAGIC    oss_mcc  ,
# MAGIC    oss_mnc  ,
# MAGIC    oss_lac  ,
# MAGIC    oss_cell_id  ,
# MAGIC    cell_operational_state_desc  ,
# MAGIC    cell_administration_state_desc  ,
# MAGIC    cell_barred_state_desc  ,
# MAGIC    sector_status_desc  ,
# MAGIC    sector_status_rank_preference  ,
# MAGIC    oss_parent_obj_cd  ,
# MAGIC    oss_parent_obj_type_desc  ,
# MAGIC    oss_site_equip_cd  ,
# MAGIC    oss_site_equip_type_desc  ,
# MAGIC    oss_parent_equip_mkt_cd  ,
# MAGIC    oss_mkt_name  ,
# MAGIC    reason_desc  ,
# MAGIC    alarm_text  ,
# MAGIC    last_sync_dt  ,
# MAGIC    test_cell_flg  ,
# MAGIC    source_created_by_id  ,
# MAGIC    source_created_dt  ,
# MAGIC    source_modified_by_id  ,
# MAGIC    source_modified_dt  ,
# MAGIC    nortel_site_cd  ,
# MAGIC    oss_enodeb_id  ,
# MAGIC    oss_tac  ,
# MAGIC    oss_pci  ,
# MAGIC    oss_ecgi  ,
# MAGIC    node_type_desc
# MAGIC    )
# MAGIC VALUES (
# MAGIC inc.oss_cell_info_id  ,
# MAGIC    inc.site_cd  ,
# MAGIC    inc.sector_cd  ,
# MAGIC    inc.sector_technology_desc  ,
# MAGIC    inc.oss_vendor  ,
# MAGIC    inc.source_oss_server_desc  ,
# MAGIC    inc.network_hierarchy  ,
# MAGIC    inc.oss_site_name  ,
# MAGIC    inc.oss_sector_name  ,
# MAGIC    inc.oss_cell_name  ,
# MAGIC    inc.oss_parent_equip_cd  ,
# MAGIC    inc.parent_equip_cd  ,
# MAGIC    inc.parent_equip_type_desc  ,
# MAGIC    inc.oss_cgi  ,
# MAGIC    inc.oss_mcc  ,
# MAGIC    inc.oss_mnc  ,
# MAGIC    inc.oss_lac  ,
# MAGIC    inc.oss_cell_id  ,
# MAGIC    inc.cell_operational_state_desc  ,
# MAGIC    inc.cell_administration_state_desc  ,
# MAGIC    inc.cell_barred_state_desc  ,
# MAGIC    inc.sector_status_desc  ,
# MAGIC    inc.sector_status_rank_preference  ,
# MAGIC    inc.oss_parent_obj_cd  ,
# MAGIC    inc.oss_parent_obj_type_desc  ,
# MAGIC    inc.oss_site_equip_cd  ,
# MAGIC    inc.oss_site_equip_type_desc  ,
# MAGIC    inc.oss_parent_equip_mkt_cd  ,
# MAGIC    inc.oss_mkt_name  ,
# MAGIC    inc.reason_desc  ,
# MAGIC    inc.alarm_text  ,
# MAGIC    inc.last_sync_dt  ,
# MAGIC    inc.test_cell_flg  ,
# MAGIC    inc.source_created_by_id  ,
# MAGIC    inc.source_created_dt  ,
# MAGIC    inc.source_modified_by_id  ,
# MAGIC    inc.source_modified_dt  ,
# MAGIC    inc.nortel_site_cd  ,
# MAGIC    inc.oss_enodeb_id  ,
# MAGIC    inc.oss_tac  ,
# MAGIC    inc.oss_pci  ,
# MAGIC    inc.oss_ecgi  ,
# MAGIC    inc.node_type_desc
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW changed_association_oss_cell_info AS
# MAGIC SELECT a.* FROM association_oss_cell_info a
# MAGIC INNER JOIN updated_and_inserted_rows c on (c.OSS_CGI = a.OSS_CGI) and (c.NETWORK_HIERARCHY = a.NETWORK_HIERARCHY) and (c.OSS_VENDOR = a.OSS_VENDOR);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_dwh_association_cell_info_incremental
# MAGIC --ORDER BY 1 DESC

# COMMAND ----------

# MAGIC %run ./5A-Incremental_Export_to_Azure_SQL_DB