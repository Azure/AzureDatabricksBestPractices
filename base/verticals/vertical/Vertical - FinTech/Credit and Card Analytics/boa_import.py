# Databricks notebook source
from pyspark.sql.functions import udf
from pyspark.sql.types import *
# It's very important to put this import before keras,
# as explained here: Loading tensorflow before scipy.misc seems to cause imread to fail #1541
# https://github.com/tensorflow/tensorflow/issues/1541
import scipy.misc

import tensorflow as tf
import keras
import pandas as pd
from keras import backend as K
from keras.applications import InceptionV3
from keras.applications import imagenet_utils
from keras.applications.inception_v3 import preprocess_input
from keras.preprocessing.image import img_to_array
from keras.preprocessing.image import load_img
from keras.models import load_model
import numpy as np
import requests
import os
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors, VectorUDT
from keras.models import model_from_json
from pyspark.sql import Row
from pyspark.sql.functions import struct
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from sklearn.metrics import roc_curve, auc

from pyspark.sql.functions import udf, lit, struct
from pyspark.sql.types import *

from neurospark.image.imageIO import readImages
from neurospark.transformers.named_image import DeepImageFeaturizer 
from neurospark.image.imageIO import readImages

# COMMAND ----------

def _proba_neg(v, prediction):
  return float(v.array[prediction])
proba_neg = udf(_proba_neg, DoubleType())

# COMMAND ----------

# MAGIC %scala
# MAGIC package org.neurospark
# MAGIC 
# MAGIC import org.apache.spark.sql.Row
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC import javax.imageio.ImageIO
# MAGIC import java.awt.image.BufferedImage
# MAGIC import java.io.ByteArrayOutputStream
# MAGIC import org.imgscalr.Scalr
# MAGIC import java.util.Base64
# MAGIC 
# MAGIC /**
# MAGIC  * All the code related to images.
# MAGIC  */
# MAGIC object ImageCode {
# MAGIC   case class MyImage(
# MAGIC       mode: String,
# MAGIC       height: Int,
# MAGIC       width: Int,
# MAGIC       data: Array[Byte])
# MAGIC 
# MAGIC   def rowToImage(row: Row): BufferedImage = row match {
# MAGIC     case Row(mode: String, height: Int, width: Int, data: Array[Byte]) =>
# MAGIC       rowToImage0(MyImage(mode, height, width, data))
# MAGIC     case Row(mode: String, height: Int, width: Int, numChannels: Int, data: Array[Byte]) =>
# MAGIC       rowToImage0(MyImage(mode, height, width, data))
# MAGIC   }
# MAGIC 
# MAGIC   def rowToImage0(img: MyImage): BufferedImage = {
# MAGIC     import img._
# MAGIC     assert(mode == "RGB", mode)
# MAGIC     assert(width > 0, width)
# MAGIC     assert(height > 0, height)
# MAGIC     assert(data.length == 3 * width * height, (data.length, width, height))
# MAGIC     val image = new BufferedImage(width.toInt, height.toInt, BufferedImage.TYPE_INT_RGB);
# MAGIC     var pos: Int = 0
# MAGIC     while(pos + 3 < data.length) {
# MAGIC       val r = data(pos)
# MAGIC       val g = data(pos + 1)
# MAGIC       val b = data(pos + 2)
# MAGIC       val rgb = (r << 16) + (g << 8) + b;
# MAGIC       val x = (pos / 3) / width
# MAGIC       val y = (pos / 3) % width
# MAGIC       image.setRGB(y.toInt, x.toInt, rgb);
# MAGIC       pos += 3
# MAGIC     }
# MAGIC     image
# MAGIC   }
# MAGIC 
# MAGIC }
# MAGIC 
# MAGIC object ImageDisplay {
# MAGIC   /**
# MAGIC    * Takes an image, already encoded as a byte array, and
# MAGIC    */
# MAGIC   def rescaleByteArray(image: BufferedImage): BufferedImage = {
# MAGIC     val targetWidth = 150
# MAGIC     val targetHeight = 100
# MAGIC     val scaledImg: BufferedImage = Scalr.resize(image, Scalr.Method.QUALITY, Scalr.Mode.FIT_TO_WIDTH,
# MAGIC       targetWidth, targetHeight, Scalr.OP_ANTIALIAS)
# MAGIC     scaledImg
# MAGIC   }
# MAGIC 
# MAGIC   def writePng(image: BufferedImage): Array[Byte] = {
# MAGIC     val baos = new ByteArrayOutputStream()
# MAGIC     ImageIO.write(image, "png", baos)
# MAGIC     baos.flush()
# MAGIC     val imageInByte = baos.toByteArray
# MAGIC     baos.close()
# MAGIC     imageInByte
# MAGIC   }
# MAGIC 
# MAGIC   def b64(bytes: Array[Byte]): String = {
# MAGIC     val prefix = "<img src=\"data:image/png;base64,"
# MAGIC     val suffix = "\"/>"
# MAGIC     prefix + (new String(Base64.getEncoder().encode(bytes))) + suffix
# MAGIC   }
# MAGIC 
# MAGIC   // Converts an image (in the row representation) into a Base64 representation of a PNG thumbnail.
# MAGIC   def imgToB64_(row: Row): String = {
# MAGIC     val img = ImageCode.rowToImage(row)
# MAGIC     val rescaled = rescaleByteArray(img)
# MAGIC     val png = writePng(rescaled)
# MAGIC     b64(png)
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC package org.neurospark
# MAGIC 
# MAGIC import org.apache.spark.sql.{DataFrame, Row}
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC 
# MAGIC object DisplayTransforms {
# MAGIC 
# MAGIC   import ImageDisplay.imgToB64_
# MAGIC 
# MAGIC   case class DisplayRow(cells: Seq[DisplayCell])
# MAGIC   case class DisplayCell(s: String)
# MAGIC 
# MAGIC   def makeDisplayTransform(schema: StructType): Row => DisplayRow = {
# MAGIC     val trans = schema.fields.map(_.dataType).map(makeTrans0)
# MAGIC     def f(r: Row): DisplayRow = {
# MAGIC       val elts = r.toSeq.zip(trans).map { case (x, t) => t(x) }
# MAGIC       DisplayRow(elts)
# MAGIC     }
# MAGIC     f
# MAGIC   }
# MAGIC 
# MAGIC   val imageStruct = StructType(Seq(
# MAGIC     StructField("mode", StringType),
# MAGIC     StructField("height", IntegerType),
# MAGIC     StructField("width", IntegerType),
# MAGIC     StructField("data", BinaryType)
# MAGIC   ))
# MAGIC 
# MAGIC   val imageStructStrict = StructType(Seq(
# MAGIC     StructField("mode", StringType, nullable = false),
# MAGIC     StructField("height", IntegerType, nullable = false),
# MAGIC     StructField("width", IntegerType, nullable = false),
# MAGIC     StructField("data", BinaryType, nullable = false)
# MAGIC   ))
# MAGIC 
# MAGIC   val imageStructWithChans = StructType(Seq(
# MAGIC     StructField("mode", StringType),
# MAGIC     StructField("height", IntegerType),
# MAGIC     StructField("width", IntegerType),
# MAGIC     StructField("nChannels", IntegerType),
# MAGIC     StructField("data", BinaryType)
# MAGIC   ))
# MAGIC 
# MAGIC   val imageStructStrictWithChans = StructType(Seq(
# MAGIC     StructField("mode", StringType, nullable = false),
# MAGIC     StructField("height", IntegerType, nullable = false),
# MAGIC     StructField("width", IntegerType, nullable = false),
# MAGIC     StructField("nChannels", IntegerType, nullable = false),
# MAGIC     StructField("data", BinaryType, nullable = false)
# MAGIC   ))
# MAGIC 
# MAGIC   def makeTrans0(dt: DataType): Any => DisplayCell = dt match {
# MAGIC 
# MAGIC     case StringType => makeFun(dt) {
# MAGIC       case s: String => DisplayCell(s)
# MAGIC     }
# MAGIC 
# MAGIC     case IntegerType => makeFun(dt) { case i: Int => DisplayCell(i.toString) }
# MAGIC 
# MAGIC     case DoubleType => makeFun(dt) {
# MAGIC       case d: java.lang.Double => DisplayCell(d.toString)
# MAGIC       case d: Double => DisplayCell(d.toString) }
# MAGIC 
# MAGIC     case FloatType => makeFun(dt) {
# MAGIC       case f: java.lang.Float => DisplayCell(f.toString)
# MAGIC       case f: Float => DisplayCell(f.toString) }
# MAGIC 
# MAGIC     case ArrayType(dt0, _) =>
# MAGIC       val f = makeTrans0(dt0)
# MAGIC       makeFun(dt) {
# MAGIC         case s: Array[Any] => DisplayCell(s.map(f).map(_.s).mkString("[", ",", "]"))
# MAGIC         case s: Seq[Any] => DisplayCell(s.map(f).map(_.s).mkString("[", ",", "]"))
# MAGIC       }
# MAGIC 
# MAGIC     case x: StructType if x == imageStruct =>
# MAGIC       makeFun(x) { case r: Row => DisplayCell(imgToB64_(r)) }
# MAGIC 
# MAGIC     case x: StructType if x == imageStructStrict =>
# MAGIC       makeFun(x) { case r: Row => DisplayCell(imgToB64_(r)) }
# MAGIC 
# MAGIC     case x: StructType if x == imageStructWithChans =>
# MAGIC       makeFun(x) { case r: Row => DisplayCell(imgToB64_(r)) }
# MAGIC 
# MAGIC     case x: StructType if x == imageStructStrictWithChans =>
# MAGIC       makeFun(x) { case r: Row => DisplayCell(imgToB64_(r)) }
# MAGIC 
# MAGIC     case x =>
# MAGIC       throw new Exception(s"Cannot handle type $x")
# MAGIC   }
# MAGIC 
# MAGIC   def makeFun[A](dt: DataType)(fun: PartialFunction[A, DisplayCell]): A => DisplayCell = { x =>
# MAGIC     if (x == null) {
# MAGIC       DisplayCell("NULL")
# MAGIC     } else {
# MAGIC       fun.apply(x)
# MAGIC //      fun.applyOrElse(x,
# MAGIC //        throw new Exception(s"Failure when trying to use converter $dt with type ${x.getClass}" +
# MAGIC //          s": $x"))
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   def renderRow(dr: DisplayRow): String = "<tr>" + dr.cells.map { c =>
# MAGIC     "<td>" + c.s + "</td>" }.mkString("") + "</tr>"
# MAGIC 
# MAGIC   def displaySpecial(df: DataFrame, numRows: Int = 10): String = {
# MAGIC     val schema: StructType = df.schema
# MAGIC     val trans = makeDisplayTransform(schema).andThen(renderRow)
# MAGIC     val headers = schema.map(c => s"""<th class="${c.name}">${c.name}</th>""").mkString("\n")
# MAGIC     val rows = df.take(numRows).map(trans).mkString("\n")
# MAGIC     s"""
# MAGIC     <style type="text/css">
# MAGIC     </style>
# MAGIC     <table id="mlTable" class="table table-striped" style="border: 1px solid #ddd;">
# MAGIC       <thead style="background: #fafafa; color: #888">
# MAGIC         <tr>
# MAGIC           $headers
# MAGIC         </tr>
# MAGIC       </thead>
# MAGIC       ${rows}
# MAGIC     </table>
# MAGIC     """
# MAGIC   }
# MAGIC 
# MAGIC   /**
# MAGIC    * Main display function for DBC.
# MAGIC    * @param df
# MAGIC    * @param numRows
# MAGIC    */
# MAGIC   def displayML(df: org.apache.spark.sql.DataFrame, numRows: Int=20): Unit = {
# MAGIC     val str = org.neurospark.DisplayTransforms.displaySpecial(df, numRows)
# MAGIC     // GRRR this does not compile of course. When do we have a real API?
# MAGIC     //displayHTML(str)
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC /**
# MAGIC  * A wrapper class that can be accessed in python.
# MAGIC  *
# MAGIC  * In needs to be combined with displayHTML() in python.
# MAGIC  */
# MAGIC class PythonDisplayTransform() {
# MAGIC   def display(df: DataFrame): String = DisplayTransforms.displaySpecial(df)
# MAGIC 
# MAGIC   def display(df: DataFrame, numRows: Int): String = DisplayTransforms.displaySpecial(df, numRows)
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC // Plug into the DBC system:
# MAGIC def displayML(df: org.apache.spark.sql.DataFrame, numRows: Int=20): Unit = {
# MAGIC   val str = org.neurospark.DisplayTransforms.displaySpecial(df, numRows)
# MAGIC   displayHTML(str)
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC # Interface for python:
# MAGIC from pyspark import SparkContext
# MAGIC 
# MAGIC def _java_api(javaClassName):
# MAGIC     """
# MAGIC     Loads the PythonInterface object.
# MAGIC     """
# MAGIC     _sc = SparkContext._active_spark_context
# MAGIC     _jvm = _sc._jvm
# MAGIC     # You cannot simply call the creation of the the class on the _jvm due to classloader issues
# MAGIC     # with Py4J.
# MAGIC     return _jvm.Thread.currentThread().getContextClassLoader().loadClass(javaClassName) \
# MAGIC         .newInstance()
# MAGIC 
# MAGIC _api = _java_api("org.neurospark.PythonDisplayTransform")
# MAGIC 
# MAGIC def displayML(df, numRows = 10):
# MAGIC   dfj = df._jdf
# MAGIC   s = _api.display(dfj, numRows)
# MAGIC   displayHTML(s)

# COMMAND ----------

# MAGIC %run ./keras_udf_lib_2

# COMMAND ----------

from scipy.misc import imread
from pyspark import SparkFiles
import numpy as np
import matplotlib.pyplot as plt

path = "/tmp/thumbnails/thumbnails_features_deduped_publish/"

def showPictures(file_names, title):
  plt.close()
  for i in range(48):
    plt.subplot(6, 8, i + 1)
    try:
      plt.imshow(imread(file_names[i]), cmap=plt.cm.gray)
    except:
      # just go to the next one if the file is not present
      pass
    plt.axis("off")
    plt.suptitle(title)
  display(plt.show())

def showSamplePictures():
  sample = table("fraud.thumbnails").sample(False, 0.33, 291202L).selectExpr("name", 'img[5] as img').limit(48).rdd.map(lambda r: path + r[0]+'/'+r[1]).collect()
  showPictures(sample, "Sample Faces")
  
def showPicturesPerson(name):
  sample = table("fraud.thumbnails")
  s = sample.where(sample.name == name.lower()).selectExpr("name", 'explode(img) as img').limit(48).rdd.map(lambda r: path + r[0]+'/'+r[1]).collect()
  showPictures(s, name)
  
def showMugshot():
  plt.close()
  img = imread("/dbfs/thumbnails/images/josh brolin/16.jpg")
  plt.imshow(img)
  plt.axis("off")
  plt.suptitle("Mugshot")
  display(plt.show())

#showStimuli(files, "Title")
#showPicturesPerson('Ivanka Trump')

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.types.BinaryType
# MAGIC import java.nio.file.{Files, Paths}
# MAGIC 
# MAGIC def getByteArray(uri : String) : Array[Byte] = {
# MAGIC   Files.readAllBytes(Paths.get(uri))
# MAGIC }
# MAGIC spark.udf.register("getByteArray", getByteArray _)

# COMMAND ----------

def add_num_channels(df):
  c = df['image']
  c2 = struct(
    c.mode.alias('mode'),
    c.height.alias('height'),
    c.width.alias('width'),
    lit(3).cast(IntegerType()).alias('num_channels'),
    c.data.alias('data'))
  return df.withColumn('image2', c2).drop('image').withColumnRenamed('image2', 'image')

# COMMAND ----------

# MAGIC %sql create or replace view fraud.criminals as select * from fraud.criminal_database where gender = 'male' and name in ('matt damon', 'louis armstrong', 'jose mourinho', 'marlon brando', 'josh brolin') and image.width is not null

# COMMAND ----------

# MAGIC %sql create or replace view fraud.testing_data as select * from fraud.criminal_database where gender = 'male' and name = 'josh brolin' and image.width is not null and filename = '16.jpg'

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.joda.time.LocalDate
# MAGIC 
# MAGIC 
# MAGIC def to_date_sql(year : Int, month : Int, date : Int) : java.sql.Date = {
# MAGIC   
# MAGIC   val ld = new LocalDate(year, month, 1).plusDays(date)
# MAGIC   return new java.sql.Date(ld.toDate.getTime)
# MAGIC }
# MAGIC 
# MAGIC spark.udf.register("to_date_sql", to_date_sql _)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.joda.time.LocalDateTime
# MAGIC import org.joda.time.LocalDate
# MAGIC import org.joda.time.LocalTime
# MAGIC import org.apache.spark.sql.types.TimestampType
# MAGIC 
# MAGIC 
# MAGIC def to_sql_timestamp(year : Integer, month : Integer, date : Integer, hour : Integer, min : Integer, sec : Integer) : java.sql.Timestamp = {
# MAGIC   val ld = new LocalDate(year, month, 1).plusDays(date-1)
# MAGIC   val ts = ld.toDateTime(new LocalTime(hour, min, sec))
# MAGIC   return new java.sql.Timestamp(ts.toDateTime.getMillis())
# MAGIC }
# MAGIC 
# MAGIC spark.udf.register("to_sql_timestamp", to_sql_timestamp _)

# COMMAND ----------

# MAGIC %sql select to_sql_timestamp(2017, 1, 1, 10,20,30)

# COMMAND ----------

