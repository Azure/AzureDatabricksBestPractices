// Databricks notebook source

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

def statFileWithPartitions(input_path_para: String , partsCol: List[String],input_type:String = "parquet" ):DataFrame ={
  
    val filter_file_name = if(input_type == "parquet") "name like 'part-%'" else " 1==1"
  
    val  input_path = input_path_para +(input_path_para.takeRight(1) match{
        case "/" => ""
        case _ => "/"
    })//input_path_para has to have / at the end

    val oneMB = 1048576.0 //1MB
    val rawDF =  input_type match {
             case "parquet" => spark.read.parquet(input_path)
             case  "csv"|"tsv" => spark.read.csv(input_path)
    }
    val rawDFStat = rawDF.selectExpr(partsCol:_*).distinct
    val listOfPartitions = rawDFStat.collect().map(r => (partsCol.zip(r.toSeq).toList.map(t => t._1+"="+t._2).mkString("/"),r.toSeq.mkString("_")))

    var statArr = listOfPartitions.map{ case(lp,lv) =>{
        val partPath = lp
        val fullPath = input_path + partPath
        println(fullPath)
        val filesDF = dbutils.fs.ls(fullPath).toDF
        val statDF = filesDF
                     .where(filter_file_name)
                     .agg(count("name").as("num_file")
                         ,(max("size")/oneMB).as("max_size")
                         ,(min("size")/oneMB).as("min_size")
                         ,(avg("size")/oneMB).as("avg_size")
                         ,(sum("size")/oneMB).as("part_size")
                      )
        statDF.withColumn("partition",lit(lv))
    }}
     val statDF = statArr.reduce(_.union(_))
     statDF
}

def statFileFolder(input_path: String ):DataFrame ={
      val oneMB = 1048576.0 //1MB

      val filesDF = dbutils.fs.ls(input_path).toDF
      val statDF = filesDF.where("name like 'part-%'").agg(count("name").as("num_file")
                                                             ,(max("size")/oneMB).as("max_size")
                                                             ,(min("size")/oneMB).as("min_size")
                                                             ,(avg("size")/oneMB).as("avg_size")
                                                             ,(sum("size")/oneMB).as("part_size")
                                                            )
  statDF
}

displayHTML("""<div style="font-size:larger">
  <div>Declared utility methods:</div>
  <li><code>statFileFolder(input_path: String): DataFrame</code></li>
  <li><code>statFileWithPartitions(input_path_para: String, partsCol: List[String], input_type: String): DataFrame</code></li>
</div>""")
