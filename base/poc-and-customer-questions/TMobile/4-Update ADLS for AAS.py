# Databricks notebook source
# MAGIC %sql 
# MAGIC INSERT OVERWRITE TABLE fact_site_down_update_csv
# MAGIC SELECT `site_cd` ,
# MAGIC                  `site_dwkey` ,
# MAGIC                  `gsm_status_desc`,
# MAGIC                  `umts_status_desc` ,
# MAGIC                  `lte_status_desc` ,
# MAGIC                  `last_gsm_site_down_dt` ,
# MAGIC                  `last_gsm_site_down_reason_desc` ,
# MAGIC                  `last_lte_site_down_dt` ,
# MAGIC                  `last_lte_site_down_reason_desc` ,
# MAGIC                  `last_umts_site_down_dt` ,
# MAGIC                  `last_umts_site_down_reason_desc` ,
# MAGIC                  `last_gsm_site_up_dt` ,
# MAGIC                  `last_lte_site_up_dt` ,
# MAGIC                  `last_umts_site_up_dt` ,
# MAGIC                  `GSM_Resolve_Unknown_Flg`,
# MAGIC                  `GSM_Not_Synced_In_Last_1Hour_Flg` ,
# MAGIC                  `GSM_Insite_Only_On_Air_Flg` ,
# MAGIC                  `UMTS_Resolve_Unknown_Flg` ,
# MAGIC                  `UMTS_Not_Synced_In_Last_1Hour_Flg` ,
# MAGIC                  `UMTS_Insite_Only_On_Air_Flg` ,
# MAGIC                  `LTE_Resolve_Unknown_Flg` ,
# MAGIC                  `LTE_Not_Synced_In_Last_1Hour_Flg` ,
# MAGIC                  `LTE_Insite_Only_On_Air_Flg` ,
# MAGIC                  `GSM_LAST_SYNC_DT` ,
# MAGIC                  `UMTS_LAST_SYNC_DT` ,
# MAGIC                  `LTE_LAST_SYNC_DT` ,
# MAGIC                  `gsm_sector_off` ,
# MAGIC                  `umts_sector_off` ,
# MAGIC                  `lte_sector_off` ,
# MAGIC                  CAST(`last_gsm_available_dt` AS DATE),
# MAGIC                  CAST(`last_umts_available_dt` AS DATE),
# MAGIC                  CAST(`last_lte_available_dt` AS DATE)
# MAGIC FROM fact_site_down_update_csv

# COMMAND ----------

display(spark.sql("SELECT * FROM final_cooked_table_csv"))