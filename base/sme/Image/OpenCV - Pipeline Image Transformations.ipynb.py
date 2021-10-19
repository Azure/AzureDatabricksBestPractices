# Databricks notebook source
# MAGIC %md ## 302 - Pipeline Image Transformation with OpenCV
# MAGIC 
# MAGIC This example shows how to manipulate the collection of images.
# MAGIC First, the images are downloaded to the local directory.
# MAGIC Second, they are copied to your cluster's attached HDFS.

# COMMAND ----------

# MAGIC %md The images are loaded from the directory (for fast prototyping, consider loading a fraction of
# MAGIC images). Inside the dataframe, each image is a single field in the image column. The image has
# MAGIC sub-fields (path, height, width, OpenCV type and OpenCV bytes).

# COMMAND ----------

import mmlspark
import numpy as np
from mmlspark import toNDArray

imageDir = "wasbs://publicwasb@mmlspark.blob.core.windows.net/sampleImages"
images = spark.readImages(imageDir, recursive = True, sampleRatio = 0.1).cache()
images.printSchema()
print(images.count())

# COMMAND ----------

# MAGIC %md We can also alternatively stream the images with a similiar api.
# MAGIC Check the [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC for more details on streaming.

# COMMAND ----------

imageStream = spark.streamImages(imageDir + "/*", sampleRatio = 0.1)
query = imageStream.select("image.height").writeStream.format("memory").queryName("heights").start()
print("Streaming query activity: {}".format(query.isActive))

# COMMAND ----------

# MAGIC %md Wait a few seconds and then try querying for the images below.
# MAGIC Note that when streaming a directory of images that already exists it will
# MAGIC consume all images in a single batch. If one were to move images into the
# MAGIC directory, the streaming engine would pick up on them and send them as
# MAGIC another batch.

# COMMAND ----------

heights = spark.sql("select * from heights")
print("Streamed {} heights".format(heights.count()))

# COMMAND ----------

# MAGIC %md After we have streamed the images we can stop the query:

# COMMAND ----------

query.stop()

# COMMAND ----------

# MAGIC %md When collected from the *DataFrame*, the image data are stored in a *Row*, which is Spark's way
# MAGIC to represent structures (in the current example, each dataframe row has a single Image, which
# MAGIC itself is a Row).  It is possible to address image fields by name and use `toNDArray()` helper
# MAGIC function to convert the image into numpy array for further manipulations.

# COMMAND ----------

from PIL import Image

data = images.take(3)    # take first three rows of the dataframe
im = data[2][0]          # the image is in the first column of a given row

print("image type: {}, number of fields: {}".format(type(im), len(im)))
print("image path: {}".format(im.path))
print("height: {}, width: {}, OpenCV type: {}".format(im.height, im.width, im.type))

arr = toNDArray(im)     # convert to numpy array
Image.fromarray(arr, "RGB")   # display the image inside notebook
print(images.count())

# COMMAND ----------

# MAGIC %md Use `ImageTransformer` for the basic image manipulation: resizing, cropping, etc.
# MAGIC Internally, operations are pipelined and backed by OpenCV implementation.

# COMMAND ----------

from mmlspark import ImageTransformer

tr = (ImageTransformer()                  # images are resized and then cropped
      .setOutputCol("transformed")
      .resize(height = 200, width = 200)
      .crop(0, 0, height = 180, width = 180) )

small = tr.transform(images).select("transformed")

im = small.take(3)[2][0]                  # take third image
Image.fromarray(toNDArray(im), "RGB")   # display the image inside notebook

# COMMAND ----------

# MAGIC %md For the advanced image manipulations, use Spark UDFs.
# MAGIC The MMLSpark package provides conversion function between *Spark Row* and
# MAGIC *ndarray* image representations.

# COMMAND ----------

from pyspark.sql.functions import udf
from mmlspark import ImageSchema, toNDArray, toImage

def u(row):
    array = toNDArray(row)    # convert Image to numpy ndarray[height, width, 3]
    array[:,:,2] = 0
    return toImage(array)     # numpy array back to Spark Row structure

noBlueUDF = udf(u,ImageSchema)

noblue = small.withColumn("noblue", noBlueUDF(small["transformed"])).select("noblue")

im = noblue.take(3)[2][0]                # take second image
Image.fromarray(toNDArray(im), "RGB")   # display the image inside notebook

# COMMAND ----------

# MAGIC %md Images could be unrolled into the dense 1D vectors suitable for CNTK evaluation.

# COMMAND ----------

from mmlspark import UnrollImage

unroller = UnrollImage().setInputCol("noblue").setOutputCol("unrolled")

unrolled = unroller.transform(noblue).select("unrolled")

vector = unrolled.take(1)[0][0]
print(type(vector))
len(vector.toArray())

# COMMAND ----------

