# Databricks notebook source
# MAGIC %md # Conversion of the Transaction SQL to  Spark SQL 
# MAGIC 
# MAGIC The following conversions are needed to make the SQL executable on databricks
# MAGIC * Conversion of common table expressions to temporary views
# MAGIC * Conversion of _datediff_ for one hour to _to_unix_timestamp(last_sync_dt) < (to_unix_timestamp(now())-3600)_
# MAGIC * Conversion of _isnull()_ to _ifnull_. Spark SQL has an isnull function but its meaning is different
# MAGIC * Conversion of _GETUTCDATE()_ to _current_timestamp()_. Note it is assumed that _GETUTCDATE()-1_ means 1 day before current time
# MAGIC * Column renaming when using pivot operations
# MAGIC * Conversion of _isnumeric(x) == 0_ to _CAST(left(sr.sector_cd,1) as INT) is null_
# MAGIC * Conversion of _isnumeric(x) == 1_ to _CAST(left(sr.sector_cd,1) as INT) is not null_
# MAGIC 
# MAGIC The conversion of of the common 

# COMMAND ----------

# MAGIC %md last_sync_dt

# COMMAND ----------

# note db syntax does not accept name for pivot clause
spark.sql("""create or replace temporary view last_sync_dt as
   select site_cd, GSM as GSM_last_sync_dt, LTE as LTE_last_sync_dt, UMTS as UMTS_last_sync_dt from  
   (
     select site_cd, sector_technology_desc, last_sync_dt
      from association_oss_cell_info ci 
      where
        --site_dwkey>0 --and ci.last_sync_dt >=dateadd(HH,-1,GETUTCDATE())
        --and
        test_cell_flg = 0
      ) as T 
   pivot (
        max(last_sync_dt) as last_sync_dt
      for sector_technology_desc in ('GSM','UMTS', 'LTE')
      ) 
  distribute by site_cd
""")

# TODO: validate null vals is valid case for GSM_last_sync_dt
#display(spark.sql("select * from last_sync_dt where GSM_last_sync_dt is not null"))

# COMMAND ----------

spark.sql("""
create or replace temporary view site_gsm_master as
  select  T.site_cd,
          T.sector_technology_desc,
          T.sector_status_desc,
          T.source_modified_dt,
          T.reason_desc,
          T.last_sync_dt,
          T.Not_Synced_In_Last_1Hour_Flg,
          oss_vendor from 
 ( select
    site_cd, 
    sector_technology_desc,
    sector_status_desc,
    source_modified_dt,    
    reason_desc,
    last_sync_dt, 
    case when to_unix_timestamp(last_sync_dt) < (to_unix_timestamp(now())-3600) then 'Y' else 'N' end as Not_Synced_In_Last_1Hour_Flg,
    ROW_NUMBER ( )
    OVER (PARTITION BY site_cd,sector_technology_desc order by sector_status_rank_preference asc,last_sync_dt desc) as row_num,
    oss_vendor
    from association_oss_cell_info ci 
  where
    --site_dwkey>0 --and ci.last_sync_dt >=dateadd(HH,-1,GETUTCDATE())
    --and
    test_cell_flg = 0
    --and last_sync_dt >= dateadd(HH,-2,GETUTCDATE()) --GETUTCDATE()-1
    ) T
    where   T.row_num=1 and T.sector_technology_desc = 'GSM' 
    order by Not_Synced_In_Last_1Hour_Flg desc, site_cd desc
""")

#display(spark.sql("select * from site_gsm_master"))

# COMMAND ----------

spark.sql("""
create or replace temporary view site_umts_master as
  select  T.site_cd,
          T.sector_technology_desc,
          T.sector_status_desc,
          T.source_modified_dt,
          T.reason_desc,
          T.last_sync_dt,
          T.Not_Synced_In_Last_1Hour_Flg,
          oss_vendor from 
 ( select
    site_cd, 
    sector_technology_desc,
    sector_status_desc,
    source_modified_dt,    
    reason_desc,
    last_sync_dt, 
    case when to_unix_timestamp(last_sync_dt) < (to_unix_timestamp(now())-3600) then 'Y' else 'N' end as Not_Synced_In_Last_1Hour_Flg,
    ROW_NUMBER ( )
    OVER (PARTITION BY site_cd,sector_technology_desc order by sector_status_rank_preference asc,last_sync_dt desc) as row_num,
    oss_vendor
    from association_oss_cell_info ci 
  where
    --site_dwkey>0 --and ci.last_sync_dt >=dateadd(HH,-1,GETUTCDATE())
    --and
    test_cell_flg = 0
    --and last_sync_dt >= dateadd(HH,-2,GETUTCDATE()) --GETUTCDATE()-1
    ) T
    where   T.row_num=1 and T.sector_technology_desc = 'UMTS' 
    order by Not_Synced_In_Last_1Hour_Flg desc, site_cd desc
""")

#display(spark.sql("select * from site_umts_master"))

# COMMAND ----------

spark.sql("""
create or replace temporary view site_lte_master as
  select  T.site_cd,
          T.sector_technology_desc,
          T.sector_status_desc,
          T.source_modified_dt,
          T.reason_desc,
          T.last_sync_dt,
          T.Not_Synced_In_Last_1Hour_Flg,
          oss_vendor from 
 ( select
    site_cd, 
    sector_technology_desc,
    sector_status_desc,
    source_modified_dt,    
    reason_desc,
    last_sync_dt, 
    case when to_unix_timestamp(last_sync_dt) < (to_unix_timestamp(now())-3600) then 'Y' else 'N' end as Not_Synced_In_Last_1Hour_Flg,
    ROW_NUMBER ( )
    OVER (PARTITION BY site_cd,sector_technology_desc order by sector_status_rank_preference asc,last_sync_dt desc) as row_num,
    oss_vendor
    from association_oss_cell_info ci 
  where
    --site_dwkey>0 --and ci.last_sync_dt >=dateadd(HH,-1,GETUTCDATE())
    --and
    test_cell_flg = 0
    --and last_sync_dt >= dateadd(HH,-2,GETUTCDATE()) --GETUTCDATE()-1
    ) T
    where   T.row_num=1 and T.sector_technology_desc = 'LTE' 
    order by Not_Synced_In_Last_1Hour_Flg desc, site_cd desc
""")

#display(spark.sql("select * from site_lte_master"))

# COMMAND ----------

spark.sql("""create or replace temporary view site_on_air_ct as
       select  sr.site_cd,
        sum(case when CAST(left(sr.sector_cd,1) as INT) is null and sr.sector_status in ('OFF-AIR','INACTIVE') then 1 else 0 end) as GSM_OFF_AIR_COUNT,
       sum(case when CAST(left(sr.sector_cd,1) as INT) is not null and sr.sector_cd not like '%L%' and sr.sector_status in ('OFF-AIR','INACTIVE') then 1 else 0 end) as UMTS_OFF_AIR_COUNT,
       sum(case when CAST(left(sr.sector_cd,1) as INT) is not null and sr.sector_cd like '%L%' and sr.sector_status in ('OFF-AIR','INACTIVE') then 1 else 0 end) as LTE_OFF_AIR_COUNT,
       sum(case when CAST(left(sr.sector_cd,1) as INT) is not null then 1 else 0 end) as gsm_count,
       sum(case when CAST(left(sr.sector_cd,1) as INT) is not null and sr.sector_cd not like '%L%' then 1 else 0 end) as umts_count,
       sum(case when CAST(left(sr.sector_cd,1) as INT) is not null and sr.sector_cd like '%L%' then 1 else 0 end ) as lte_count
       from dim_sector sr 
       where active_flg='Y'
       group by  sr.site_cd
""")

#display(spark.sql("select * from site_on_air_ct"))

# COMMAND ----------

# MAGIC %md ## Final data build
# MAGIC Now build the final data output

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC set spark.sql.shuffle.partitions=64;
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE final_cooked_table
# MAGIC select
# MAGIC       fs.site_cd,
# MAGIC       fs.site_dwkey,
# MAGIC       case
# MAGIC                     when onair.gsm_count>0 and onair.GSM_OFF_AIR_COUNT=gsm_count then 'Not Available'
# MAGIC                     --when gsm.sector_status_desc is null and lsd.gsm_LAST_SYNC_DT <  dateadd(HH,-2,GETUTCDATE()) then 'LoOSS'
# MAGIC                     when gsm.sector_status_desc is null and fs.gsm_site_on_air_dt is not null and fs.gsm_site_off_air_dt is null then 'Not Discovered'
# MAGIC                     when gsm.sector_status_desc is not null then gsm.sector_status_desc
# MAGIC          else 'Not Available'
# MAGIC          end as gsm_status_desc,
# MAGIC       case
# MAGIC                     when onair.umts_count>0 and onair.UMTS_OFF_AIR_COUNT=umts_count then 'Not Available'
# MAGIC                     --when umts.sector_status_desc is null and lsd.umts_LAST_SYNC_DT <  dateadd(HH,-2,GETUTCDATE()) then 'LoOSS'
# MAGIC                     when umts.sector_status_desc is null and fs.umts_site_on_air_dt is not null and fs.umts_site_off_air_dt is null then 'Not Discovered'
# MAGIC                     when umts.sector_status_desc is not null then umts.sector_status_desc
# MAGIC                     else 'Not Available'
# MAGIC          end as umts_status_desc,
# MAGIC       case
# MAGIC                     when onair.lte_count>0 and onair.LTE_OFF_AIR_COUNT=lte_count then 'Not Available'
# MAGIC                     when gsm.sector_status_desc ='DOWN' and umts.sector_status_desc ='DOWN' and lte.sector_status_desc='UNKNOWN' then 'DOWN'
# MAGIC                     --when lte.sector_status_desc is null and lsd.lte_LAST_SYNC_DT <  dateadd(HH,-2,GETUTCDATE()) then 'LoOSS'
# MAGIC                     when lte.sector_status_desc is null and fs.lte_site_on_air_dt is not null and fs.lte_site_off_air_dt is null then 'Not Discovered'
# MAGIC                     when lte.sector_status_desc is not null then lte.sector_status_desc
# MAGIC                     else  'Not Available'
# MAGIC          end as lte_status_desc,
# MAGIC 
# MAGIC       case when gsm.sector_status_desc='DOWN' then gsm.source_modified_dt else fsd.last_gsm_site_down_dt end as last_gsm_site_down_dt,
# MAGIC       gsm.reason_desc as last_gsm_site_down_reason_desc,
# MAGIC 
# MAGIC          case
# MAGIC              when gsm.sector_status_desc ='DOWN' and umts.sector_status_desc ='DOWN' and lte.sector_status_desc='UNKNOWN' then umts.source_modified_dt
# MAGIC              when lte.sector_status_desc='DOWN' then lte.source_modified_dt
# MAGIC          else fsd.last_lte_site_down_dt end as last_lte_site_down_dt,
# MAGIC 
# MAGIC       lte.reason_desc as last_lte_site_down_reason_desc,
# MAGIC       case when umts.sector_status_desc='DOWN' then umts.source_modified_dt else fsd.last_umts_site_down_dt end as last_umts_site_down_dt,
# MAGIC       umts.reason_desc as last_umts_site_down_reason_desc, 
# MAGIC       case when gsm.sector_status_desc='UP' then gsm.source_modified_dt else fsd.last_gsm_site_up_dt end as last_gsm_site_up_dt,
# MAGIC       case when lte.sector_status_desc='UP' then lte.source_modified_dt else fsd.last_lte_site_up_dt end as last_lte_site_up_dt,
# MAGIC       case when umts.sector_status_desc='UP' then umts.source_modified_dt else fsd.last_umts_site_up_dt end as last_umts_site_up_dt,
# MAGIC 
# MAGIC       case when gsm.sector_status_desc = 'UNKNOWN' then 'Y' else 'N' end as GSM_Resolve_Unknown_Flg,
# MAGIC       gsm.Not_Synced_In_Last_1Hour_Flg as GSM_Not_Synced_In_Last_1Hour_Flg,
# MAGIC       -- check if GETUTCDATE()-1) == (to_unix_timestamp(now())-86400)
# MAGIC       case when ((gsm.sector_status_desc is null and fs.gsm_site_on_air_dt is not null) or to_unix_timestamp(gsm.last_sync_dt) < (to_unix_timestamp(now())-86400)) then 'Y' else 'N' end as GSM_Insite_Only_On_Air_Flg,
# MAGIC 
# MAGIC       case when umts.sector_status_desc = 'UNKNOWN' then 'Y' else 'N' end as UMTS_Resolve_Unknown_Flg,
# MAGIC       umts.Not_Synced_In_Last_1Hour_Flg as UMTS_Not_Synced_In_Last_1Hour_Flg,
# MAGIC       case when ((umts.sector_status_desc is null and fs.umts_site_on_air_dt is not null) or to_unix_timestamp(umts.last_sync_dt) < (to_unix_timestamp(now())-86400)) then 'Y' else 'N' end as UMTS_Insite_Only_On_Air_Flg,
# MAGIC 
# MAGIC       case when lte.sector_status_desc = 'UNKNOWN' then 'Y' else 'N' end as LTE_Resolve_Unknown_Flg,
# MAGIC       lte.Not_Synced_In_Last_1Hour_Flg as LTE_Not_Synced_In_Last_1Hour_Flg,
# MAGIC       case when ((lte.sector_status_desc is null and fs.lte_site_on_air_dt is not null) or to_unix_timestamp(lte.last_sync_dt) < (to_unix_timestamp(now())-86400)) then 'Y' else 'N' end as LTE_Insite_Only_On_Air_Flg,
# MAGIC       lsd.GSM_LAST_SYNC_DT,
# MAGIC       lsd.UMTS_LAST_SYNC_DT,
# MAGIC       lsd.LTE_LAST_SYNC_DT,
# MAGIC          gsm_all_sectors_off_air_flg = case when onair.gsm_count>0 and onair.GSM_OFF_AIR_COUNT=gsm_count then 'Y' else 'N' end AS gsm_sector_off,
# MAGIC       umts_all_sectors_off_air_flg = case when onair.umts_count>0 and onair.UMTS_OFF_AIR_COUNT=umts_count then 'Y' else 'N' end AS umts_sector_off,
# MAGIC          lte_all_sectors_off_air_flg = case when onair.lte_count>0 and onair.LTE_OFF_AIR_COUNT=lte_count then 'Y' else 'N' end AS lte_sector_off,
# MAGIC 
# MAGIC          case when
# MAGIC                case
# MAGIC                            when onair.gsm_count>0 and onair.GSM_OFF_AIR_COUNT=gsm_count then 'Not Available'
# MAGIC                            when gsm.sector_status_desc is null and fs.gsm_site_on_air_dt is not null and fs.gsm_site_off_air_dt is null then 'Not Discovered'
# MAGIC                            when gsm.sector_status_desc is not null then gsm.sector_status_desc
# MAGIC                else 'Not Available'
# MAGIC                end not in ('UP','Not Discovered','Not Available') then ifnull(fsd.last_gsm_available_dt,current_timestamp())
# MAGIC 
# MAGIC          when gsm.sector_status_desc = 'UP' then null
# MAGIC          else
# MAGIC          fsd.last_gsm_available_dt
# MAGIC          end as last_gsm_available_dt,
# MAGIC 
# MAGIC          case when
# MAGIC              case
# MAGIC                     when onair.umts_count>0 and onair.UMTS_OFF_AIR_COUNT=umts_count then 'Not Available'
# MAGIC                     when umts.sector_status_desc is null and fs.umts_site_on_air_dt is not null and fs.umts_site_off_air_dt is null then 'Not Discovered'
# MAGIC                     when umts.sector_status_desc is not null then umts.sector_status_desc
# MAGIC                     else 'Not Available'
# MAGIC                end not in ('UP','Not Discovered','Not Available') then ifnull(fsd.last_umts_available_dt,current_timestamp())
# MAGIC                when umts.sector_status_desc ='UP' then null
# MAGIC          else
# MAGIC                fsd.last_umts_available_dt
# MAGIC          end as last_umts_available_dt,
# MAGIC 
# MAGIC          case when
# MAGIC              case
# MAGIC                     when onair.lte_count>0 and onair.LTE_OFF_AIR_COUNT=lte_count then 'Not Available'
# MAGIC                     when gsm.sector_status_desc ='DOWN' and umts.sector_status_desc ='DOWN' and lte.sector_status_desc='UNKNOWN' then 'DOWN'
# MAGIC                     when lte.sector_status_desc is null and fs.lte_site_on_air_dt is not null and fs.lte_site_off_air_dt is null then 'Not Discovered'
# MAGIC                     when lte.sector_status_desc is not null then lte.sector_status_desc
# MAGIC                     else  'Not Available'
# MAGIC              end not in ('UP','Not Discovered','Not Available') then ifnull(fsd.last_lte_available_dt,current_timestamp())
# MAGIC              when lte.sector_status_desc ='UP' then null
# MAGIC        else
# MAGIC              fsd.last_lte_available_dt                 
# MAGIC         end as last_lte_available_dt
# MAGIC from
# MAGIC 
# MAGIC (select * from dim_site distribute by site_cd) fs 
# MAGIC left outer join site_gsm_master gsm on fs.site_cd = gsm.site_cd
# MAGIC        and ((fs.gsm_site_on_air_dt is not null and fs.gsm_site_off_air_dt is null and fs.site_status_desc ='ON-AIR') or fs.site_class_desc='COW')
# MAGIC left outer join site_umts_master umts on fs.site_cd = umts.site_cd and ((fs.umts_site_on_air_dt is not null and fs.umts_site_off_air_dt is null and fs.site_status_desc ='ON-AIR') or fs.site_class_desc='COW')
# MAGIC left outer join site_lte_master lte on fs.site_cd = lte.site_cd and ((fs.lte_site_on_air_dt is not null and fs.lte_site_off_air_dt is null and fs.site_status_desc ='ON-AIR') or fs.site_class_desc='COW')
# MAGIC left outer join (select * from fact_site_down distribute by site_cd) fsd  on fs.site_cd = fsd.site_cd
# MAGIC left outer join site_on_air_ct  onair on fsd.site_cd = onair.site_cd
# MAGIC left outer join last_sync_dt lsd on fs.site_cd=lsd.site_cd
# MAGIC where 
# MAGIC --fs.site_on_air_dt is not null and
# MAGIC fs.row_active_flg='Y' --and fs.site_cd='NEB1001A'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Fix date formatting
# MAGIC SELECT count(*) FROM final_cooked_table
# MAGIC WHERE site_cd LIKE "%NY%"