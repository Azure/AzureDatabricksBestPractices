# Databricks notebook source
# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
# MAGIC val jdbcUsername = "NDWPOCAdmin" //dbutils.secrets.get(scope = "jdbc", key = "username")
# MAGIC val jdbcPassword = "AccentureMagenta2018!" //dbutils.secrets.get(scope = "jdbc", key = "password")
# MAGIC val jdbcHostname = "ndwpocsqldbsrv.database.windows.net"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "ndwpocsqldb"
# MAGIC 
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC 
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC 
# MAGIC connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC connectionProperties.put("password", s"${jdbcPassword}")
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC 
# MAGIC val df_export = spark.sql(""" 
# MAGIC           SELECT site_cd
# MAGIC                 ,sector_cd
# MAGIC                 ,sector_technology_desc
# MAGIC                 ,oss_vendor
# MAGIC                 ,source_oss_server_desc
# MAGIC                 ,network_hierarchy
# MAGIC                 ,oss_site_name
# MAGIC                 ,oss_sector_name
# MAGIC                 ,oss_cell_name
# MAGIC                 ,oss_parent_equip_cd
# MAGIC                 ,parent_equip_cd
# MAGIC                 ,parent_equip_type_desc
# MAGIC                 ,oss_cgi
# MAGIC                 ,oss_mcc
# MAGIC                 ,oss_mnc
# MAGIC                 ,oss_lac
# MAGIC                 ,oss_cell_id
# MAGIC                 ,cell_operational_state_desc
# MAGIC                 ,cell_administration_state_desc
# MAGIC                 ,cell_barred_state_desc
# MAGIC                 ,sector_status_desc
# MAGIC                 ,sector_status_rank_preference
# MAGIC                 ,oss_parent_obj_cd
# MAGIC                 ,oss_parent_obj_type_desc
# MAGIC                 ,oss_site_equip_cd
# MAGIC                 ,oss_site_equip_type_desc
# MAGIC                 ,oss_parent_equip_mkt_cd
# MAGIC                 ,oss_mkt_name
# MAGIC                 ,reason_desc
# MAGIC                 ,alarm_text
# MAGIC                 ,last_sync_dt
# MAGIC                 ,test_cell_flg
# MAGIC                 ,source_created_by_id
# MAGIC                 ,source_created_dt
# MAGIC                 ,source_modified_by_id
# MAGIC                 ,source_modified_dt
# MAGIC                 ,nortel_site_cd
# MAGIC                 ,oss_enodeb_id
# MAGIC                 ,oss_tac
# MAGIC                 ,oss_pci
# MAGIC                 ,oss_ecgi
# MAGIC                 ,node_type_desc
# MAGIC       FROM association_oss_cell_info
# MAGIC       """)     
# MAGIC 
# MAGIC df_export.write
# MAGIC        .mode(SaveMode.Append)
# MAGIC        .jdbc(jdbcUrl, "dbo.association_oss_cell_info_databricks", connectionProperties)

# COMMAND ----------

##%sql
# oss_parent_obj_cd
#SELECT * FROM association_oss_cell_info_changes

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.sql.DriverManager
# MAGIC 
# MAGIC // Call sproc
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
# MAGIC val conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC val proc_stmt = conn.prepareCall("{ call dbo.sp_Merge_association_oss_cell_info() }")
# MAGIC proc_stmt.execute()

# COMMAND ----------

#%run ./5A-Incremental_Export_to_Azure_SQL_DB