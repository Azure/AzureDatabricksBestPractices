// Databricks notebook source
// MAGIC %md TensorFramesStubs.scala

// COMMAND ----------

package org.apache.spark.sql.tfs_stubs

import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, Row, SparkSession}

class TFUDF(
    opName: String,
    fun: Column => (Any => Row),
    returnType: DataType)
  extends UserDefinedFunction(null, returnType, None) {

  override def apply(exprs: Column*): Column = {
    assert(exprs.size == 1, exprs)
    val f = fun(exprs.head)
    val inner = exprs.toSeq.map(_.toString()).mkString(", ")
    val name = s"$opName($inner)"
    Column(ScalaUDF(f, dataType, exprs.map(_.expr), Nil)).alias(name)
  }
}

class PipelinedUDF(
    opName: String,
    udfs: Seq[UserDefinedFunction], returnType: DataType)
  extends UserDefinedFunction(null, returnType, None) {
  assert(udfs.nonEmpty)

  override def apply(exprs: Column*): Column = {
    val start = udfs.head.apply(exprs: _*)
    var rest = start
    for (udf <- udfs.tail) {
      rest = udf.apply(rest)
    }
    val inner = exprs.toSeq.map(_.toString()).mkString(", ")
    val name = s"$opName($inner)"
    rest.alias(name)
  }
}

object TFUDF {
  def make1(opName: String, fun: Column => (Any => Row), returnType: DataType): TFUDF = {
    new TFUDF(opName, fun, returnType)
  }

  def makeUDF[U,V](f: U => V, returnType: DataType): UserDefinedFunction = {
    UserDefinedFunction(f, returnType, None)
  }

  def pipeline(opName: String, udf1: UserDefinedFunction, udfs: UserDefinedFunction*): UserDefinedFunction = {
    if (udfs.isEmpty) {
      udf1
    } else {
      new PipelinedUDF(opName, Seq(udf1) ++ udfs, udfs.last.dataType)
    }
  }
}

object TensorFramesUDF {
  def registerUDF(spark: SparkSession, name: String, udf: UserDefinedFunction): UserDefinedFunction = {
    def builder(children: Seq[Expression]) = udf.apply(children.map(Column.apply) : _*).expr
    spark.sessionState.functionRegistry.registerFunction(name, builder)
    udf
  }
}


// COMMAND ----------

// MAGIC %md SqlOps.scala

// COMMAND ----------

package org.tensorframes.impl

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.tfs_stubs.TFUDF
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.StructType
import org.tensorflow.Session
import org.tensorflow.framework.GraphDef
import org.tensorflow.Graph
import org.tensorframes.{ColumnInformation, Shape, ShapeDescription, _}


/**
 * Column transforms. These are not as efficient as working with full dataframes,
 * but they may be simpler to work with.
 */
// TODO: this is mostly cut and paste from performMapRows -> should combine
object SqlOps extends Logging {
  import SchemaTransforms.{get, check}

  // Counter: the number of sessions currently opened.
  private class LocalState(
      val session: Session,
      val graphHash: Int,
      val graph: Graph,
      val counter: AtomicInteger) {
    def close(): Unit = {
      session.close()
      graph.close()
    }
  }

  // A map of graph hash -> state for this graph.
  private[this] var current: Map[Int, LocalState] = Map.empty
  private[this] val lock = new Object()

  // The maximum number of sessions that can be opened concurrently.
  val maxSessions: Int = 10

  /**
   * Experimental: expresses a TensorFlow Row transform as a SQL-registrable UDF.
   *
   * This is not as efficient as doing a direct dataframe transform, and it leaks
   * some resources after the completion of the transform. These resources may easily be
   * be reclaimed though.
   *
   * @param udfName: the name of the UDF that is going to be presented when building the udf.
   * @param graph the graph of the computation
   * @param shapeHints the extra info regarding the shapes
   * @param applyBlocks: if true, the graph is assumed to accepted vectorized inputs.
   * @param flattenStruct: if true, and if the return type contains a single field, then
   *                     this field will be exposed as the value, instead of returning a struct.
   */
  def makeUDF(
      udfName: String,
      graph: GraphDef,
      shapeHints: ShapeDescription,
      applyBlocks: Boolean,
      flattenStruct: Boolean): UserDefinedFunction = {

    val (outputSchema, udf1) = makeUDF0(udfName, graph, shapeHints, applyBlocks = applyBlocks)
    outputSchema match {
      case StructType(Array(f1)) if flattenStruct =>
        // Flatten the structure around the field
        // For now, the only reasonable way is to create another UDF :(
        def fun(r: Row): Any = r.get(0)
        val udf2 = TFUDF.makeUDF(fun, f1.dataType)
        TFUDF.pipeline(udfName, udf1, udf2)
      case _ =>
        udf1
    }
  }

  /**
   * Experimental: expresses a Row transform as a SQL-registrable UDF.
   *
   * This is not as efficient as doing a direct dataframe transform, and it leaks
   * some resources after the completion of the transform. These resources may easily be
   * be reclaimed though.
   *
   * @param udfName: the name of the UDF that is going to be presented when building the udf.
   * @param graph the graph of the computation
   * @param shapeHints the extra info regarding the shapes
   * @param applyBlocks: if true, the graph is assumed to accepted vectorized inputs.
   *
   * Returns the UDF and the schema of the output (always a struct)
   */
  def makeUDF0(
      udfName: String,
      graph: GraphDef,
      shapeHints: ShapeDescription,
      applyBlocks: Boolean): (StructType, UserDefinedFunction) = {
    val summary = TensorFlowOps.analyzeGraphTF(graph, shapeHints)
      .map(x => x.name -> x).toMap
    val inputs = summary.filter(_._2.isInput)
    val outputs = summary.filter(_._2.isOutput)

    // The output schema of the block from the data generated by TF.
    val outputTFSchema: StructType = {
      // The order of the output columns is decided for now by their names.
      val fields = outputs.values.toSeq.sortBy(_.name).map { out =>
        // Compute the shape of the block. If the data is blocked, there is no need to append an extra dimension.
        val blockShape = if (applyBlocks) { out.shape } else {
          // The shapes we get in each output node are the shape of the cells of each column, not the
          // shape of the column. Add Unknown since we do not know the exact length of the block.
          out.shape.prepend(Shape.Unknown)
        }
        ColumnInformation.structField(out.name, out.scalarType, blockShape)
      }
      StructType(fields.toArray)
    }

    val outputSchema: StructType = StructType(outputTFSchema)

    def processColumn(inputColumn: Column): Any => Row = {
      // Special case: if the column has a single non-structural field and if
      // there is a single input in the graph, we automatically wrap the input in a structure.
      (inputs.keySet.toSeq, inputColumn.expr.dataType) match {
        case (_, _: StructType) => processColumn0(inputColumn)
        case (Seq(name1), _) => processColumn0(struct(inputColumn.alias(name1)))
        case (names, dt) =>
          throw new Exception(s"Too many graph inputs for the given column type: names=$names, dt=$dt")
      }
    }

    def processColumn0(inputColumn: Column): Any => Row = {
      val inputSchema = inputColumn.expr.dataType match {
        case st: StructType => st
        case x: Any => throw new Exception(
          s"Only structures are currently accepted: given $x")
      }
      val fieldsByName = inputSchema.fields.map(f => f.name -> f).toMap
      val cols = inputSchema.fieldNames.mkString(", ")

      // Initial check of the input.
      inputs.values.foreach { in =>
        val fname = get(shapeHints.inputs.get(in.name),
          s"The graph placeholder ${in.name} was not given a corresponding dataframe field name as input:" +
            s"hints: ${shapeHints.inputs}")

        val f = get(fieldsByName.get(fname),
          s"Graph input ${in.name} found, but no column to match it. Dataframe columns: $cols")

        val stf = get(ColumnInformation(f).stf,
          s"Data column ${f.name} has not been analyzed yet, cannot run TF on this dataframe")

        check(stf.dataType == in.scalarType,
          s"The type of node '${in.name}' (${stf.dataType}) is not compatible with the data type " +
            s"of the column (${in.scalarType})")

        val cellShape = stf.shape.tail
        if (applyBlocks) {
          check(in.shape.numDims >= 1,
          s"The input '${in.name}' is expected to at least a vector, but it currently scalar")
          // Check against the tail (which should be the cell).
          check(cellShape.checkMorePreciseThan(in.shape.tail),
            s"The data column '${f.name}' has shape ${stf.shape} (not compatible) with shape" +
              s" ${in.shape} requested by the TF graph")
        } else {
          val cellShape = stf.shape.tail
          // No check for unknowns: we allow unknowns in the first dimension of the cell shape.
          check(cellShape.checkMorePreciseThan(in.shape),
            s"The data column '${f.name}' has shape ${stf.shape} (not compatible) with shape" +
              s" ${in.shape} requested by the TF graph")
        }

        check(in.isPlaceholder,
          s"Invalid type for input node ${in.name}. It has to be a placeholder")
      }

      // The column indices requested by TF, and the name of the placeholder that gets fed.
      val requestedTFInput: Array[(NodePath, Int)] = {
        val colIdxs = inputSchema.fieldNames.zipWithIndex.toMap
        inputs.keys.map { nodePath =>
          val fieldName = shapeHints.inputs(nodePath)
          nodePath -> colIdxs(fieldName)
        }   .toArray
      }

      logger.debug(s"makeUDF: input schema = $inputSchema, requested cols: ${requestedTFInput.toSeq}" +
        s" complete output schema = $outputSchema")
      // TODO: this is leaking the file.
      val sc = SparkContext.getOrCreate()
      val gProto = sc.broadcast(TensorFlowOps.graphSerial(graph))
      val f = performUDF(inputSchema, requestedTFInput, gProto, outputTFSchema, applyBlocks)
      f
    }

    outputSchema -> TFUDF.make1(udfName, processColumn, outputSchema)
  }

  def performUDF(
    inputSchema: StructType,
    inputTFCols: Array[(NodePath, Int)],
    g_bc: Broadcast[SerializedGraph],
    tfOutputSchema: StructType,
    applyBlocks: Boolean): Any => Row = {

    logger.debug(s"performUDF: inputSchema=$inputSchema inputTFCols=${inputTFCols.toSeq}")

    def f(in: Any): Row = {
      val row = in match {
        case r: Row => r
        case x => Row(x)
      }
      val g = g_bc.value
      retrieveSession(g) { session =>
        g.evictContent()
        val inputTensors = TFDataOps.convert(row, inputSchema, inputTFCols)
        logger.debug(s"performUDF:inputTensors=$inputTensors")
        val requested = tfOutputSchema.map(_.name)
        var runner = session.runner()
        for (req <- requested) {
          runner = runner.fetch(req)
        }
        for ((inputName, inputTensor) <- inputTensors) {
          runner = runner.feed(inputName, inputTensor)
        }
        val outs = runner.run().asScala
        logger.debug(s"performUDF:outs=$outs")
        // Close the inputs
        inputTensors.map(_._2).foreach(_.close())
        val res = TFDataOps.convertBack(outs, tfOutputSchema, Array(row), inputSchema, appendInput = false)
        // Close the outputs
        outs.foreach(_.close())
        assert(res.hasNext)
        val r = res.next()
        assert(!res.hasNext)
        //      logDebug(s"performUDF: r=$r")
        r
      }
    }
    f
  }

  private def retrieveSession[T](g: SerializedGraph)(f: Session => T): T = {
    //  // This is a better version that uses the hash of the content, but it requires changes to
    //  // TensorFrames. Only use with version 0.2.9+
    val hash = java.util.Arrays.hashCode(g.content)
    retrieveSession(g, hash, f)
  }

  private def retrieveSession[T](g: SerializedGraph, gHash: Int, f: Session => T): T = {
    // Do some cleanup first:
    lock.synchronized {
      val numberOfSessionsToClose = Math.max(current.size - maxSessions, 0)
      // This is best effort only, there may be more sessions opened at some point.
      if (numberOfSessionsToClose > 0) {
        // Find some sessions to close: they are not currently used, and they are not the requested session.
        val sessionsToRemove = current.valuesIterator
          .filter { s => s.counter.get() == 0 && s.graphHash != gHash }
          .take(numberOfSessionsToClose)
        for (state <- sessionsToRemove) {
          logger.debug(s"Removing session ${state.graphHash}")
          state.close()
          current = current - state.graphHash
        }
      }
    }

    // Now, try to retrieve the session, or create a new one.
    // TODO: use a double lock mechanism or a lazy value, since importing a graph may take a long time.
    val state = lock.synchronized {
      val state0 = current.get(gHash) match {
        case None =>
          // Add a new session
          val tg = new Graph()
          tg.importGraphDef(g.content)
          val s = new Session(tg)
          val ls = new LocalState(s, gHash, tg, new AtomicInteger(0))
          current = current + (gHash -> ls)
          ls
        case Some(ls) =>
          // Serve the existing session
          ls
      }
      // Increment the counter in the locked section, to guarantee that the session does not get collected.
      state0.counter.incrementAndGet()
      state0
    }

    // Perform the action
    try {
      f(state.session)
    } finally {
      state.counter.decrementAndGet()
    }
  }
}


// COMMAND ----------

// MAGIC %md ImageOps.scala

// COMMAND ----------

package org.tensorframes.impl_images

import java.net.URL
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.tfs_stubs.TFUDF
import org.tensorflow.framework.GraphDef
import org.tensorframes.impl.SqlOps
import org.tensorframes.{Logging, ShapeDescription}

import scala.collection.JavaConverters._

object FileOps extends Logging {
  val modelDownloaded = new AtomicBoolean(false)

  val localModelPath: String = {
    Paths.get(
      System.getProperty("java.io.tmpdir"), "inceptionv3-" + UUID.randomUUID().toString).toString
  }

  def downloadFile(url: String): Array[Byte] = {
    // A hack to install model by hijacking donwload file ...
    downloadAndInstallModel(localModelPath)

    val stream = new URL(url).openStream()
    val bytes = org.apache.commons.io.IOUtils.toByteArray(stream)
    stream.close()
    bytes
  }

  def downloadAndInstallModel(localPath: String): Unit = {
    def download(sourceUrl: String): Unit = {
      val url = new URL(sourceUrl)
      val localFile = Paths.get(localPath, FilenameUtils.getName(url.getFile)).toFile
      localFile.getParentFile.mkdirs()
      FileUtils.copyURLToFile(url, localFile)
    }

    // Download the model, but only do it once per process.
    if (!modelDownloaded.get) {
      modelDownloaded.synchronized {
        if (!modelDownloaded.get) {
          download("http://home.apache.org/~rxin/models/inceptionv3/classes.txt")
          download("http://home.apache.org/~rxin/models/inceptionv3/main.pb")
          download("http://home.apache.org/~rxin/models/inceptionv3/preprocessor.pb")
          modelDownloaded.set(true)
        }
      }
    }
  }
}

/**
 * Experimental class that contains the image-related code.
 */
object ImageOps extends Logging {

  def postprocessUDF(classes: Seq[String], threshold: Double): UserDefinedFunction = {
    def f2(input: Row): Array[String] = input match {
      case Row(x: Seq[Any]) =>
        assert(x.size == classes.size, s"output row has size ${x.size} but we have ${classes.size}")
        val x2 = x.map {
          case z: Float => z.toDouble
          case z: Double => z.toDouble
          case z => throw new Exception(s"Cannot cast element of type ${z.getClass}: $z")
        }
        val cs = classes.zip(x2).filter(_._2 > threshold).map(_._1)
        logger.debug(s"Found classes: $cs")
        cs.toArray
      case x =>
        throw new Exception(s"Expected row with array of double, got $x")
    }

    val returnType = ArrayType(StringType, containsNull = false)

    val udf2 = TFUDF.makeUDF(f2, returnType)
    udf2
  }

  def makeImageClassifier(): UserDefinedFunction = {
    makeImageClassifier(FileOps.localModelPath)
  }


  /**
   * Makes an image classifier from an existing model. The model must be exported from Python first.
   * @param directory the directory that contains the .protobuf files with the preprocessor and the main
   *                  model. It is going to look for:
   *                   - preprocessor.pb
   *                   - main.pb
   * @return a full classifier that is using by default the ImageNet set of labels.
   */
  def makeImageClassifier(directory: String): UserDefinedFunction = {
    val preprocessorGraph = {
      val bytes = Files.readAllBytes(Paths.get(directory + "/preprocessor.pb"))
      GraphDef.parseFrom(bytes)
    }
    val graph = {
      val bytes = Files.readAllBytes(Paths.get(directory + "/main.pb"))
      GraphDef.parseFrom(bytes)
    }
    val preproHints = ShapeDescription(Map(), List("output"), Map("image_input" -> "image_input"))
    val mainHints = ShapeDescription(Map(), List("prediction_vector"), Map("image_input" -> "output"))
    val hints = Files.readAllLines(
      Paths.get(directory+"/classes.txt"), Charset.defaultCharset()).asScala.map(_.split(" ").toSeq).flatMap {
      case Seq(id: String, name: String) => Some(id.toInt -> name)
      case _ => None
    }
    val maxKey = hints.map(_._1).max
    val m = hints.toMap

    val classes = (0 to maxKey).map { x => m.get(x).getOrElse("none_" + x) }
    makeImageClassifier(preprocessorGraph, preproHints, graph, mainHints, classes)
  }

  def makeImageClassifier(
      preprocessGraph: GraphDef,
      preprocessShapeHints: ShapeDescription,
      graph: GraphDef,
      shapeHints: ShapeDescription,
      classes: Seq[String]): UserDefinedFunction = {
    val udf0 = SqlOps.makeUDF("classify_image_0", preprocessGraph, preprocessShapeHints,
      applyBlocks = false, flattenStruct = false)
    val udf1 = SqlOps.makeUDF("classify_image_1", graph, shapeHints,
      applyBlocks = false, flattenStruct = false)
    val udf2 = postprocessUDF(classes, 0.7)
    TFUDF.pipeline("classify_image", udf0, udf1, udf2)
  }
}


// COMMAND ----------

// MAGIC %md ModelFactory.scala

// COMMAND ----------

package org.tensorframes.impl_images

import java.nio.file.{Files, Paths}
import java.util

import org.apache.log4j.PropertyConfigurator

import scala.collection.JavaConverters._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.tfs_stubs.{TFUDF, TensorFramesUDF}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.tensorflow.framework.GraphDef
import org.tensorframes.{Logging, Shape, ShapeDescription}
import org.tensorframes.impl.{SerializedGraph, SqlOps, TensorFlowOps}

/**
 * Small, python-accessible wrapper to load and register models in Spark.
 */
class ModelFactory {

}

// TODO: merge with the python factory eventually, this is essentially copy/paste
class PythonModelFactory() extends Logging {
  private var _shapeHints: ShapeDescription = ShapeDescription.empty
  // TODO: this object may leak because of Py4J -> do not hold to large objects here.
  private var _graph: SerializedGraph = null
  private var _graphPath: Option[String] = None
  private var _sqlCtx: SQLContext = null

  def initialize_logging(): Unit = initialize_logging("org/tensorframes/log4j.properties")

  /**
   * Performs some logging initialization before spark has the time to do it.
   *
   * Because of the the current implementation of PySpark, Spark thinks it runs as an interactive
   * console and makes some mistake when setting up log4j.
   */
  private def initialize_logging(file: String): Unit = {
    Option(this.getClass.getClassLoader.getResource(file)) match {
      case Some(url) =>
        PropertyConfigurator.configure(url)
      case None =>
        System.err.println(s"$this Could not load logging file $file")
    }
  }


  def shape(
      shapeHintsNames: util.ArrayList[String],
      shapeHintShapes: util.ArrayList[util.ArrayList[Int]]): this.type = {
    val s = shapeHintShapes.asScala.map(_.asScala.toSeq).map(x => Shape(x: _*))
    _shapeHints = _shapeHints.copy(out = shapeHintsNames.asScala.zip(s).toMap)
    this
  }

  def fetches(fetchNames: util.ArrayList[String]): this.type = {
    _shapeHints = _shapeHints.copy(requestedFetches = fetchNames.asScala)
    this
  }

  def graph(bytes: Array[Byte]): this.type = {
    _graph = SerializedGraph.create(bytes)
    this
  }

  def graphFromFile(filename: String): this.type = {
    _graphPath = Option(filename)
    this
  }

  def sqlContext(ctx: SQLContext): this.type = {
    _sqlCtx = ctx
    this
  }

  def inputs(
      placeholderPaths: util.ArrayList[String],
      fieldNames: util.ArrayList[String]): this.type = {
    require(placeholderPaths.size() == fieldNames.size(), (placeholderPaths.asScala, fieldNames.asScala))
    val map = placeholderPaths.asScala.zip(fieldNames.asScala).toMap
    _shapeHints = _shapeHints.copy(inputs = map)
    this
  }

  /**
   * Builds a java UDF based on the following input.
   *
   * This is taken away from PythonInterface
   *
   */
  // TODO: merge with PythonInterface. It is easier to keep separate for the time being.
  def makeUDF(udfName: String, applyBlocks: Boolean): UserDefinedFunction = {
    SqlOps.makeUDF(udfName, buildGraphDef(), _shapeHints,
      applyBlocks=applyBlocks, flattenStruct = true)
  }

  /**
   * Builds a java UDF based on the following input.
   *
   * This is taken away from PythonInterface
   *
   */
  // TODO: merge with PythonInterface. It is easier to keep separate for the time being.
  def makeUDF(udfName: String, applyBlocks: Boolean, flattenStruct: Boolean): UserDefinedFunction = {
    SqlOps.makeUDF(udfName, buildGraphDef(), _shapeHints,
      applyBlocks=applyBlocks, flattenStruct = flattenStruct)
  }

  /**
   * Registers a TF UDF under the given name in Spark.
   * @param udfName the name of the UDF
   * @param blocked indicates that the UDF should be applied block-wise.
   * @return
   */
  def registerUDF(udfName: String, blocked: java.lang.Boolean): UserDefinedFunction = {
    assert(_sqlCtx != null)
    val udf = makeUDF(udfName, blocked)
    logger.warn(s"Registering udf $udfName -> $udf to session ${_sqlCtx.sparkSession}")
    TensorFramesUDF.registerUDF(_sqlCtx.sparkSession, udfName, udf)
  }

  def registerCompositeUDF(udfName: String, judfs: java.util.ArrayList[UserDefinedFunction]): UserDefinedFunction = {
    val udfs = judfs.asScala
    val udf = TFUDF.pipeline(udfName, udfs.head, udfs.tail: _*)
    logger.warn(s"Registering composite udf $udfName -> $udf to session ${_sqlCtx.sparkSession}")
    TensorFramesUDF.registerUDF(_sqlCtx.sparkSession, udfName, udf)
  }

  private def buildGraphDef(): GraphDef = {
    _graphPath match {
      case Some(p) =>
        val path = Paths.get(p)
        val bytes = Files.readAllBytes(path)
        TensorFlowOps.readGraphSerial(SerializedGraph.create(bytes))
      case None =>
        assert(_graph != null)
        TensorFlowOps.readGraphSerial(_graph)
    }
  }

}


// COMMAND ----------

// MAGIC %python
// MAGIC import logging
// MAGIC 
// MAGIC from pyspark import RDD, SparkContext
// MAGIC from pyspark.sql import SQLContext, Row, DataFrame
// MAGIC 
// MAGIC from tensorframes.core import _check_fetches, _get_graph, _add_graph, _get_shape, _add_inputs
// MAGIC 
// MAGIC #__all__ = ['registerUDF']
// MAGIC 
// MAGIC logger = logging.getLogger('tensorframes')
// MAGIC 
// MAGIC 
// MAGIC def _java_api_sql(javaClassName = "org.tensorframes.impl_images.PythonModelFactory", sqlCtx = None):
// MAGIC     """
// MAGIC     Loads the PythonInterface object (lazily, because the spark context needs to be initialized
// MAGIC     first).
// MAGIC     """
// MAGIC     _sc = SparkContext._active_spark_context
// MAGIC     logger.info("Spark context = " + str(_sc))
// MAGIC     if sqlCtx is None:
// MAGIC         # Try to retrieve it from the active spark context
// MAGIC         if '_active_sql_context' not in dir(SQLContext):
// MAGIC             # As a last resort, we create a new one, and we keep it around.
// MAGIC             SQLContext._active_sql_context = SQLContext(_sc)
// MAGIC         _sql = SQLContext._active_sql_context
// MAGIC     else:
// MAGIC         _sql = sqlCtx
// MAGIC     _jvm = _sc._jvm
// MAGIC     # You cannot simply call the creation of the the class on the _jvm due to classloader issues
// MAGIC     # with Py4J.
// MAGIC     return _jvm.Thread.currentThread().getContextClassLoader().loadClass(javaClassName) \
// MAGIC         .newInstance().sqlContext(_sql._ssql_ctx)
// MAGIC 
// MAGIC # TODO: merge with core
// MAGIC # Returns the names of the placeholders.
// MAGIC def _add_shapes(graph, builder, fetches):
// MAGIC     names = [fetch.name for fetch in fetches]
// MAGIC     shapes = [_get_shape(fetch) for fetch in fetches]
// MAGIC     # We still need to do the placeholders, it seems their shape is not passed in when some
// MAGIC     # dimensions are unknown
// MAGIC     ph_names = []
// MAGIC     ph_shapes = []
// MAGIC     for n in graph.as_graph_def(add_shapes=True).node:
// MAGIC         # Just the nodes that have no input.
// MAGIC         if len(list(n.input)) == 0 and str(n.op) == 'Placeholder':
// MAGIC             op_name = n.name
// MAGIC             # Simply get the default output for now, assume that the nodes have only one output
// MAGIC             t = graph.get_tensor_by_name(op_name + ":0")
// MAGIC             # Some nodes, despite our best intentions, may have undefined shapes.
// MAGIC             # Simply skip them for now.
// MAGIC             # TODO: restrict the number of nodes that we return
// MAGIC             try:
// MAGIC                 x = _get_shape(t)
// MAGIC                 ph_names.append(t.name)
// MAGIC                 ph_shapes.append(x)
// MAGIC             except ValueError:
// MAGIC                 pass
// MAGIC     print("fetches: %s %s", str(names), str(shapes))
// MAGIC     print("inputs: %s %s", str(ph_names), str(ph_shapes))
// MAGIC     builder.shape(names + ph_names, shapes + ph_shapes)
// MAGIC     builder.fetches(names)
// MAGIC     # return the path, not the tensor name.
// MAGIC     return [t_name.replace(":0", "") for t_name in ph_names]
// MAGIC 
// MAGIC def _makeUDF(fetches, name, feed_dict=None, blocked=False, register=False):
// MAGIC     fetches = _check_fetches(fetches)
// MAGIC     # We are not dealing for now with registered expansions, but this is something we should add later.
// MAGIC     graph = _get_graph(fetches)
// MAGIC     builder = _java_api_sql()
// MAGIC     _add_graph(graph, builder)
// MAGIC     ph_names = _add_shapes(graph, builder, fetches)
// MAGIC     _add_inputs(builder, feed_dict, ph_names)
// MAGIC     if register:
// MAGIC         return builder.registerUDF(name, blocked)
// MAGIC     else:
// MAGIC         return builder.makeUDF(name, blocked)
// MAGIC 
// MAGIC def registerUDF(fetches, name, feed_dict=None, blocked=False):
// MAGIC     """ Registers a transform as a SQL UDF in Spark that can then be embedded inside SQL queries.
// MAGIC 
// MAGIC     Note regarding performance and resource management: registering a TensorFlow object as a SQL UDF
// MAGIC     will open resources both on the driver (registration, etc.) and also in each of the workers: the
// MAGIC     resources used by the TensorFlow programs (GPU memory, memory buffers, etc.) will not be released
// MAGIC      until the end of the Spark application, or until another UDF is registered. The only exact way
// MAGIC     to release all the resources is currently to restart the cluster.
// MAGIC 
// MAGIC     :param fetches:
// MAGIC     :param name: the name of the UDF that will be used in SQL
// MAGIC     :param feed_dict
// MAGIC     :param blocked: if set to true, the tensorflow program is expected to manipulate blocks of data at the same time.
// MAGIC       If the number of rows returned is different than the number of rows ingested, an error is raised.
// MAGIC     :return: nothing
// MAGIC     """
// MAGIC     return _makeUDF(fetches, name, feed_dict, blocked, register=True)
// MAGIC 
// MAGIC def makeUDF(fetches, name, feed_dict=None, blocked=False):
// MAGIC     """ Makes a transform available as a python object function that can be called onto data columns.
// MAGIC 
// MAGIC     TODO: this is incomplete, it currently returns only the scala object.
// MAGIC 
// MAGIC     Note regarding performance and resource management: registering a TensorFlow object as a SQL UDF
// MAGIC     will open resources both on the driver (registration, etc.) and also in each of the workers: the
// MAGIC     resources used by the TensorFlow programs (GPU memory, memory buffers, etc.) will not be released
// MAGIC      until the end of the Spark application, or until another UDF is registered. The only exact way
// MAGIC     to release all the resources is currently to restart the cluster.
// MAGIC 
// MAGIC     :param fetches:
// MAGIC     :param name: the name of the UDF that will be used in SQL
// MAGIC     :param feed_dict
// MAGIC     :param blocked: if set to true, the tensorflow program is expected to manipulate blocks of data at the same time.
// MAGIC       If the number of rows returned is different than the number of rows ingested, an error is raised.
// MAGIC     :return: nothing
// MAGIC     """
// MAGIC     return _makeUDF(fetches, name, feed_dict, blocked, register=False)
// MAGIC 
// MAGIC def registerCompositeUDF(name, udfs, sqlContext = None):
// MAGIC     _java_api_sql(sqlCtx=sqlContext).registerCompositeUDF(name, udfs)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql import SQLContext
// MAGIC SQLContext._active_sql_context = sqlContext

// COMMAND ----------

