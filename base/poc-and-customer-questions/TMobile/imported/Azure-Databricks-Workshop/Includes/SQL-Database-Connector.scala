// Databricks notebook source
package com.databricks.training.azure.sqlserver

import scala.util.Random
import java.sql.DriverManager

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import java.sql.Connection
import java.sql.{Connection, Driver, DriverManager, JDBCType, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import java.util.Locale

/**
Usage:

spark
  .write
  .format("com.databricks.training.azure.sqlserver")
  .option("url", sqlDwUrl)
  .option("dbtable", "fruits")
  .save()
*/
class DefaultSource extends JdbcRelationProvider {

  override def shortName(): String = "azuresql"

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    val options = new JDBCOptions(parameters)
    val isCaseSensitive = false

    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      val tableExists = JdbcUtils.tableExists(conn, options)
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options)
              val tableSchema = JdbcUtils.getSchemaOption(conn, options)
              MyJdbcUtils.saveTable(df, tableSchema, isCaseSensitive, options)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table)
              createTable(conn, df, options)
              MyJdbcUtils.saveTable(df, Some(df.schema), isCaseSensitive, options)
            }

          case SaveMode.Append =>
            val tableSchema = JdbcUtils.getSchemaOption(conn, options)
            MyJdbcUtils.saveTable(df, tableSchema, isCaseSensitive, options)

          case SaveMode.ErrorIfExists =>
            throw new AnalysisException(
              s"Table or view '${options.table}' already exists. SaveMode: ErrorIfExists.")

          case SaveMode.Ignore =>
            // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
            // to not save the contents of the DataFrame and to not change the existing data.
            // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        createTable(conn, df, options)
        MyJdbcUtils.saveTable(df, Some(df.schema), isCaseSensitive, options)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }
}

object MyJdbcUtils {
    /**
   * Saves the RDD to the database in a single transaction.
   */
  def saveTable(
      df: DataFrame,
      tableSchema: Option[StructType],
      isCaseSensitive: Boolean,
      options: JDBCOptions): Unit = {
    val url = options.url
    val table = options.table
    val dialect = JdbcDialects.get(url)
    val rddSchema = df.schema
    val getConnection: () => Connection = createConnectionFactory(options)
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val insertStmt = getInsertStatement(table, rddSchema, tableSchema, isCaseSensitive, dialect)
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw new IllegalArgumentException(
        s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
          "via JDBC. The minimum value is 1.")
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    repartitionedDF.rdd.foreachPartition(iterator => savePartition(
      getConnection, table, iterator, rddSchema, insertStmt, batchSize, dialect, isolationLevel)
    )
  }
  
  /**
   * Saves a partition of a DataFrame to the JDBC database.  This is done in
   * a single database transaction (unless isolation level is "NONE")
   * in order to avoid repeatedly inserting data as much as possible.
   *
   * It is still theoretically possible for rows in a DataFrame to be
   * inserted into the database more than once if a stage somehow fails after
   * the commit occurs but before the stage can return successfully.
   *
   * This is not a closure inside saveTable() because apparently cosmetic
   * implementation changes elsewhere might easily render such a closure
   * non-Serializable.  Instead, we explicitly close over all variables that
   * are used.
   */
  def savePartition(
      getConnection: () => Connection,
      table: String,
      iterator: Iterator[Row],
      rddSchema: StructType,
      insertStmt: String,
      batchSize: Int,
      dialect: JdbcDialect,
      isolationLevel: Int): Iterator[Byte] = {
    val conn = getConnection()
    var committed = false

    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      val metadata = conn.getMetaData
      if (metadata.supportsTransactions()) {
        // Update to at least use the default isolation, if any transaction level
        // has been chosen and transactions are supported
        val defaultIsolation = metadata.getDefaultTransactionIsolation
        finalIsolationLevel = defaultIsolation
        if (metadata.supportsTransactionIsolationLevel(isolationLevel))  {
          // Finally update to actually requested level if possible
          finalIsolationLevel = isolationLevel
        }
      }
    }
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val stmt0 = conn.createStatement()
      try {
        stmt0.executeUpdate(s"SET IDENTITY_INSERT $table ON;")
      } catch {
        case ex: java.sql.SQLException => true
      }
      val stmt = conn.prepareStatement(insertStmt)
      val setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
      val nullTypes = rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              setters(i).apply(stmt, row, i)
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      try {
        stmt0.executeUpdate(s"SET IDENTITY_INSERT $table OFF;")
      } catch {
        case ex: java.sql.SQLException => true
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
      Iterator.empty
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (e.getCause == null) {
            try {
              e.initCause(cause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(cause)
            }
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        conn.close()
      }
    }
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit

  private def makeSetter(
      conn: Connection,
      dialect: JdbcDialect,
      dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setString(pos + 1, row.getString(pos))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos))

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase(Locale.ROOT).split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        val array = conn.createArrayOf(
          typeName,
          row.getSeq[AnyRef](pos).toArray)
        stmt.setArray(pos + 1, array)

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }
}


// COMMAND ----------

println("")
println("IMPORTANT: You can safely ignore any Warning messages above.")
println("Setup Complete.")