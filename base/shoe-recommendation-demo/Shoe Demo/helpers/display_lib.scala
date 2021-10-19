// Databricks notebook source
// MAGIC %md display_images.scala

// COMMAND ----------

package org.neurospark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import org.imgscalr.Scalr
import java.util.Base64

/**
 * All the code related to images.
 */
object ImageCode {
  case class MyImage(
      mode: String,
      height: Int,
      width: Int,
      data: Array[Byte])

  def rowToImage(row: Row): BufferedImage = row match {
    case Row(mode: String, height: Int, width: Int, data: Array[Byte]) =>
      rowToImage0(MyImage(mode, height, width, data))
    case Row(mode: String, height: Int, width: Int, numChannels: Int, data: Array[Byte]) =>
      rowToImage0(MyImage(mode, height, width, data))
  }

  def rowToImage0(img: MyImage): BufferedImage = {
    import img._
    assert(mode == "RGB", mode)
    assert(width > 0, width)
    assert(height > 0, height)
    assert(data.length == 3 * width * height, (data.length, width, height))
    val image = new BufferedImage(width.toInt, height.toInt, BufferedImage.TYPE_INT_RGB);
    var pos: Int = 0
    while(pos + 3 < data.length) {
      val r = data(pos)
      val g = data(pos + 1)
      val b = data(pos + 2)
      val rgb = (r << 16) + (g << 8) + b;
      val x = (pos / 3) / width
      val y = (pos / 3) % width
      image.setRGB(y.toInt, x.toInt, rgb);
      pos += 3
    }
    image
  }

}

object ImageDisplay {
  /**
   * Takes an image, already encoded as a byte array, and
   */
  def rescaleByteArray(image: BufferedImage): BufferedImage = {
    val targetWidth = 150
    val targetHeight = 100
    val scaledImg: BufferedImage = Scalr.resize(image, Scalr.Method.QUALITY, Scalr.Mode.FIT_TO_WIDTH,
      targetWidth, targetHeight, Scalr.OP_ANTIALIAS)
    scaledImg
  }

  def writePng(image: BufferedImage): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    ImageIO.write(image, "png", baos)
    baos.flush()
    val imageInByte = baos.toByteArray
    baos.close()
    imageInByte
  }

  def b64(bytes: Array[Byte]): String = {
    val prefix = "<img src=\"data:image/png;base64,"
    val suffix = "\"/>"
    prefix + (new String(Base64.getEncoder().encode(bytes))) + suffix
  }

  // Converts an image (in the row representation) into a Base64 representation of a PNG thumbnail.
  def imgToB64_(row: Row): String = {
    val img = ImageCode.rowToImage(row)
    val rescaled = rescaleByteArray(img)
    val png = writePng(rescaled)
    b64(png)
  }
}

// COMMAND ----------

// MAGIC %md DisplayTransforms.scala

// COMMAND ----------

package org.neurospark

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.SQLDataTypes._

object DisplayTransforms {

  import ImageDisplay.imgToB64_
  
  case class DisplayRow(cells: Seq[DisplayCell])
  case class DisplayCell(s: String)

  def makeDisplayTransform(schema: StructType): Row => DisplayRow = {
    val trans = schema.fields.map(_.dataType).map(makeTrans0)
    def f(r: Row): DisplayRow = {
      val elts = r.toSeq.zip(trans).map { case (x, t) => t(x) }
      DisplayRow(elts)
    }
    f
  }

  val imageStruct = StructType(Seq(
    StructField("mode", StringType),
    StructField("height", IntegerType),
    StructField("width", IntegerType),
    StructField("data", BinaryType)
  ))

  val imageStructStrict = StructType(Seq(
    StructField("mode", StringType, nullable = false),
    StructField("height", IntegerType, nullable = false),
    StructField("width", IntegerType, nullable = false),
    StructField("data", BinaryType, nullable = false)
  ))

  val imageStructWithChans = StructType(Seq(
    StructField("mode", StringType),
    StructField("height", IntegerType),
    StructField("width", IntegerType),
    StructField("nChannels", IntegerType),
    StructField("data", BinaryType)
  ))

  val imageStructStrictWithChans = StructType(Seq(
    StructField("mode", StringType, nullable = false),
    StructField("height", IntegerType, nullable = false),
    StructField("width", IntegerType, nullable = false),
    StructField("nChannels", IntegerType, nullable = false),
    StructField("data", BinaryType, nullable = false)
  ))

        // /* if x == VectorUDT().sqlType() */ =>
  def makeTrans0(dt: DataType): Any => DisplayCell = dt match {

    // DOESN'T WORK
//    case VectorType => makeFun(dt) { case v: Vector => DisplayCell(v.toString) }

    case StringType => makeFun(dt) { case s: String => DisplayCell(s) }

    case IntegerType => makeFun(dt) { case i: Int => DisplayCell(i.toString) }

    case DoubleType => makeFun(dt) {
      case d: java.lang.Double => DisplayCell(d.toString)
      case d: Double => DisplayCell(d.toString) }

    case FloatType => makeFun(dt) {
      case f: java.lang.Float => DisplayCell(f.toString)
      case f: Float => DisplayCell(f.toString) }

    case ArrayType(dt0, _) =>
      val f = makeTrans0(dt0)
      makeFun(dt) {
        case s: Array[Any] => DisplayCell(s.map(f).map(_.s).mkString("[", ",", "]"))
        case s: Seq[Any] => DisplayCell(s.map(f).map(_.s).mkString("[", ",", "]"))
      }

    case x: StructType if x == imageStruct =>
      makeFun(x) { case r: Row => DisplayCell(imgToB64_(r)) }

    case x: StructType if x == imageStructStrict =>
      makeFun(x) { case r: Row => DisplayCell(imgToB64_(r)) }

    case x: StructType if x == imageStructWithChans =>
      makeFun(x) { case r: Row => DisplayCell(imgToB64_(r)) }

    case x: StructType if x == imageStructStrictWithChans =>
      makeFun(x) { case r: Row => DisplayCell(imgToB64_(r)) }

    case x =>
      throw new Exception(s"Cannot handle type $x")
  }

  def makeFun[A](dt: DataType)(fun: PartialFunction[A, DisplayCell]): A => DisplayCell = { x =>
    if (x == null) {
      DisplayCell("NULL")
    } else {
      fun.apply(x)
//      fun.applyOrElse(x,
//        throw new Exception(s"Failure when trying to use converter $dt with type ${x.getClass}" +
//          s": $x"))
    }
  }

  def renderRow(dr: DisplayRow): String = "<tr>" + dr.cells.map { c =>
    "<td>" + c.s + "</td>" }.mkString("") + "</tr>"

  def displaySpecial(df: DataFrame, numRows: Int = 10): String = {
    val schema: StructType = df.schema
    val trans = makeDisplayTransform(schema).andThen(renderRow)
    val headers = schema.map(c => s"""<th class="${c.name}">${c.name}</th>""").mkString("\n")
    val rows = df.take(numRows).map(trans).mkString("\n")
    s"""
    <style type="text/css">
    </style>
    <table id="mlTable" class="table table-striped" style="border: 1px solid #ddd;">
      <thead style="background: #fafafa; color: #888">
        <tr>
          $headers
        </tr>
      </thead>
      ${rows}
    </table>
    """
  }

  /**
   * Main display function for DBC.
   * @param df
   * @param numRows
   */
  def displayML(df: org.apache.spark.sql.DataFrame, numRows: Int=20): Unit = {
    val str = org.neurospark.DisplayTransforms.displaySpecial(df, numRows)
    // GRRR this does not compile of course. When do we have a real API?
    //displayHTML(str)
  }
}

/**
 * A wrapper class that can be accessed in python.
 *
 * In needs to be combined with displayHTML() in python.
 */
class PythonDisplayTransform() {
  def display(df: DataFrame): String = DisplayTransforms.displaySpecial(df)

  def display(df: DataFrame, numRows: Int): String = DisplayTransforms.displaySpecial(df, numRows)
}


// COMMAND ----------

import org.apache.spark.ml.linalg.SQLDataTypes._
print(VectorType)

// COMMAND ----------

// Plug into the DBC system:
def displayML(df: org.apache.spark.sql.DataFrame, numRows: Int=20): Unit = {
  val str = org.neurospark.DisplayTransforms.displaySpecial(df, numRows)
  displayHTML(str)
}

// COMMAND ----------

// MAGIC %python
// MAGIC # Interface for python:
// MAGIC from pyspark import SparkContext
// MAGIC 
// MAGIC def _java_api(javaClassName):
// MAGIC     """
// MAGIC     Loads the PythonInterface object.
// MAGIC     """
// MAGIC     _sc = SparkContext._active_spark_context
// MAGIC     _jvm = _sc._jvm
// MAGIC     # You cannot simply call the creation of the the class on the _jvm due to classloader issues
// MAGIC     # with Py4J.
// MAGIC     return _jvm.Thread.currentThread().getContextClassLoader().loadClass(javaClassName) \
// MAGIC         .newInstance()
// MAGIC 
// MAGIC _api = _java_api("org.neurospark.PythonDisplayTransform")
// MAGIC 
// MAGIC def displayML(df, numRows = 10):
// MAGIC   dfj = df._jdf
// MAGIC   s = _api.display(dfj, numRows)
// MAGIC   displayHTML(s)

// COMMAND ----------

