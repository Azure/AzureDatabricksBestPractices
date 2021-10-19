// Databricks notebook source
val cohort = "GHS_Freeze_90_VCRome" // GHS_Freeze_90_VCRome or GHS_Freeze_90_IDT
val ancestry = "EUR" // EUR or AFR
val matrix = "BT" // BT or QT

//val plink_path = s"/mnt/rgc-dbfs/dev/matrices/${cohort}/${matrix}/raw/${ancestry}/"
val plink_path_bt_eur = s"/mnt/rgc-dbfs/dev/matrices/${cohort}/BT/raw/${ancestry}/"

// COMMAND ----------

display(dbutils.fs.ls("/mnt/rgc-dbfs/dev/matrices/GHS_Freeze_90_VCRome/BT/raw/EUR/").toDF)

// COMMAND ----------

// val rawBT_Eur = spark.read
//       .option("header", "true")
//       .option("inferSchema", "true")
//       .option("delimiter", "\t")
//       .csv(plink_path_bt_eur)

// COMMAND ----------

 val rawBT_Eur = spark.read
      .option("header", "true")
      .csv(plink_path_bt_eur)
// Data is tab seperated => reads the data unseperated
// Do not use  .option("delimiter", " ")  and .option("inferSchema", "false"), due to the large number of columns they make the reading process very slow
      

// COMMAND ----------

display(rawBT_Eur)

// COMMAND ----------

rawBT_Eur.count

// COMMAND ----------

val header = rawBT_Eur.columns(0)
val idxToColName = header.split(" ").zipWithIndex.map(_.swap).toMap

// COMMAND ----------

header.split(" ").length

// COMMAND ----------

val rawInTuple = rawBT_Eur
//.repartition(600)
.flatMap(r =>{
  val lineToArr = r.getString(0).split(" ")
  val id = lineToArr(1) //IID
  val addRowID = lineToArr.map(v =>(id,v))
  val addColIndx = addRowID.zipWithIndex.map(t => (idxToColName(t._2),t._1._1,t._1._2))
  addColIndx
}).toDF("column_name","row_id","cell_value")

// COMMAND ----------

display(rawInTuple)

// COMMAND ----------

rawInTuple.count

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

