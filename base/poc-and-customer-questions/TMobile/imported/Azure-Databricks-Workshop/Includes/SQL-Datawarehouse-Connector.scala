// Databricks notebook source
package com.databricks.training.azure.datawarehouse

import scala.util.Random
import java.sql.DriverManager

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

/**
Usage:

spark
  .write
  .format("com.databricks.training.azure.datawarehouse")
  .option("url", jdbc_url_db)
  .option("blobStorageAccount", "myblobstore.blob.core.windows.net")
  .option("blobName", "myblob")
  .option("accessKey", "BABABBBBBBCCCCDDCCCPdVCpPDM/RwfN1QTrlDEX3oq0sSbNZmNPyE8By/7l9J1Z7SVa8hsKHc48qBY1tA/mgQ==")
  .option("dbtable", "DimProductCategory")
  .save()
*/
class DefaultSource extends JdbcRelationProvider {

  override def shortName(): String = "azuredw"

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    writeDfToSqlDw(sqlContext, mode, parameters, df)
    createRelation(sqlContext, parameters)
  }
  
  def createMasterKey(
      conn: java.sql.Connection): Boolean = {
    val statement = conn.createStatement

    try {
      statement.executeUpdate("CREATE MASTER KEY")
      true
    } catch {
      case ex: com.microsoft.sqlserver.jdbc.SQLServerException => true
    } finally {
      statement.close()
    }
  }

  def createAzureStorageCredential(
      conn: java.sql.Connection,
      credentialName: String,
      accessKey: String): Boolean = {
    val statement = conn.createStatement

    try {
      statement.executeUpdate(s"""
        CREATE DATABASE SCOPED CREDENTIAL $credentialName
        WITH
            IDENTITY = 'databricks_bridge',
            SECRET = '$accessKey'
        """)
      true
    } finally {
      statement.close()
    }
  }

  def createAzureStorageDataSource(
      conn: java.sql.Connection,
      dataSourceName: String,
      wasbLocation: String,
      credentialName: String): Boolean = {
    val statement = conn.createStatement

    try {
      val sql=s"""
        CREATE EXTERNAL DATA SOURCE $dataSourceName
        WITH
        (
            TYPE = HADOOP,
            LOCATION = '$wasbLocation',
            CREDENTIAL = $credentialName
        )"""
      println(sql)
      statement.executeUpdate(sql)

      true
    } finally {
      statement.close()
    }
  }

  def createExternalDelimitedTextFileFormat(
      conn: java.sql.Connection,
      formatName: String,
      fieldTerminator: String,
      stringDelimiter: String,
      dateFormat: String,
      useTypeDefault: Boolean): Boolean = {
    val statement = conn.createStatement

    try {
      statement.executeUpdate(s"""
        CREATE EXTERNAL FILE FORMAT $formatName 
        WITH 
        (
            FORMAT_TYPE = DELIMITEDTEXT,
            FORMAT_OPTIONS
            (
                FIELD_TERMINATOR = '$fieldTerminator',
                STRING_DELIMITER = '$stringDelimiter',
                DATE_FORMAT = '$dateFormat',
                USE_TYPE_DEFAULT = $useTypeDefault 
            )
        )""")

      true
    } finally {
      statement.close()
    }
  }

  def importData(
      conn: java.sql.Connection,
      strSchema: String,
      tableName: String,
      dataSourceName: String,
      formatName: String,
      relativeLocation: String,
      mode: SaveMode,
      options : JDBCOptions) = {
    import org.apache.spark.sql.jdbc.JdbcDialects
    val dialect = JdbcDialects.get(options.url)

    val dropExternalTable = s"""DROP EXTERNAL TABLE __import_$tableName"""
    
    val createExternalTable = s"""
      CREATE EXTERNAL TABLE __import_$tableName
      (
      $strSchema
      )
      WITH
      (
          LOCATION='$relativeLocation',
          DATA_SOURCE = $dataSourceName,
          FILE_FORMAT = $formatName,
          REJECT_TYPE = VALUE,
          REJECT_VALUE = 0
      )"""
    
    val createTable = s"""
      CREATE TABLE $tableName
      WITH ( DISTRIBUTION = ROUND_ROBIN )
      AS SELECT * FROM __import_$tableName
      WHERE 1=0
      OPTION (LABEL = 'Databricks CTAS: Load $tableName')"""
    
    val insertData = s"""
      INSERT INTO $tableName
      SELECT * FROM __import_$tableName"""

    val tableExists = JdbcUtils.tableExists(conn, options)
    val statement = conn.createStatement
    try {
      statement.executeUpdate(dropExternalTable)
    } catch {
      case ex: java.sql.SQLException => true
    }
    try {
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
              // In this case, we should truncate table and then load.
              statement.executeUpdate(dialect.getTruncateQuery(options.table))
              statement.executeUpdate(createExternalTable)
              try {
                statement.executeUpdate(s"SET IDENTITY_INSERT $tableName ON;")
              } catch {
                case ex: com.microsoft.sqlserver.jdbc.SQLServerException => true
              }
              statement.executeUpdate(insertData)
              try {
                statement.executeUpdate(s"SET IDENTITY_INSERT $tableName OFF")
              } catch {
                case ex: com.microsoft.sqlserver.jdbc.SQLServerException => true
              }
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              statement.executeUpdate(createExternalTable)
              dropTable(conn, options.table)
              statement.executeUpdate(createTable)
              statement.executeUpdate(insertData)
            }

          case SaveMode.Append =>
            statement.executeUpdate(createExternalTable)
            try {
              statement.executeUpdate(s"SET IDENTITY_INSERT $tableName ON;")
            } catch {
              case ex: com.microsoft.sqlserver.jdbc.SQLServerException => true
            }
            statement.executeUpdate(insertData)
            try {
              statement.executeUpdate(s"SET IDENTITY_INSERT $tableName OFF")
            } catch {
              case ex: com.microsoft.sqlserver.jdbc.SQLServerException => true
            }

          case SaveMode.ErrorIfExists =>
            throw new AnalysisException(
              s"Table or view '${options.table}' already exists. SaveMode: ErrorIfExists.")

          case SaveMode.Ignore =>
            // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
            // to not save the contents of the DataFrame and to not change the existing data.
            // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        statement.executeUpdate(createExternalTable)
        statement.executeUpdate(createTable)
        statement.executeUpdate(insertData)
      }
      true
    } finally {
      statement.close()
    }
  }

  def optimizeIndex(
      conn: java.sql.Connection,
      tableName: String): Boolean = {
    val statement = conn.createStatement

    try {
      statement.executeUpdate(s"ALTER INDEX ALL ON $tableName REBUILD;")
      true
    } finally {
      statement.close()
    }
  }

  def exportData(
      conn: java.sql.Connection,
      tableName: String,
      dataSourceName: String,
      formatName: String,
      relativeLocation: String) = {
    val statement = conn.createStatement

    try {
      statement.executeUpdate(s"""
  CREATE EXTERNAL TABLE __export_$tableName WITH
  (
      LOCATION='$relativeLocation',
      DATA_SOURCE=$dataSourceName,
      FILE_FORMAT=$formatName
  )
  AS
  SELECT * FROM $tableName
  """)

      true
    } finally {
      statement.close()
    }
  }

  def writeDfToSqlDw(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): Boolean = {

    val options = new JDBCOptions(parameters)
    val blobStorageAccount = parameters.getOrElse("blobStorageAccount", throw new IllegalArgumentException("blobStorageAccount option must be set"))
    val blobName = parameters.getOrElse("blobName", throw new IllegalArgumentException("blobName option must be set"))
    val accessKey = parameters.getOrElse("accessKey", throw new IllegalArgumentException("accessKey option must be set"))
    val destinationTableName = parameters.getOrElse("dbtable", throw new IllegalArgumentException("dbtable option must be set"))
    val jdbcUrl = options.url

    sqlContext.sparkSession.conf.set(
      s"fs.azure.account.key.$blobStorageAccount",
      accessKey)

    val transactionKey = Random.alphanumeric.take(10).mkString
    val transferContainerName = s"${destinationTableName}_$transactionKey"

    df
      .write.format("com.databricks.spark.csv")
      .save(s"wasbs://$blobName@$blobStorageAccount/$transferContainerName")

    val connection = DriverManager.getConnection(jdbcUrl)
    val storageCredentialName = s"db_storage_credential_$transactionKey"

    createMasterKey(connection)
    
    createAzureStorageCredential(
      connection,
      storageCredentialName,
      accessKey)

    val schema = JdbcUtils
      .schemaString(df, "jdbc:sqlserver")
      .replace("NVARCHAR(MAX)", "NVARCHAR(128)")

    val dataSourceName = s"sql_dw_in_data_source_$transactionKey"

    createAzureStorageDataSource(
      connection,
      dataSourceName,
      s"wasbs://$blobName@$blobStorageAccount",
      storageCredentialName)

    val csvFormatName = s"csv_format_iso_timestamp_$transactionKey"

    createExternalDelimitedTextFileFormat(
      connection,
      csvFormatName,
      ",",
      "\"",
      """yyyy-MM-dd HH:mm:ss""",
      false)

    importData(
      connection,
      schema,
      destinationTableName,
      dataSourceName,
      csvFormatName,
      s"/$transferContainerName/",
      mode,
      options)
  }
}


// COMMAND ----------

println("")
println("IMPORTANT: You can safely ignore any Warning messages above.")
println("... Setup Complete")