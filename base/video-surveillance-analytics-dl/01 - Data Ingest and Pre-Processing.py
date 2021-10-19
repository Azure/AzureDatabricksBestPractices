# Databricks notebook source
# MAGIC %md #Identifying Suspicious Behavior in Security Footage  
# MAGIC 
# MAGIC ![image](/files/mnt/raela/video_splash.png)

# COMMAND ----------

# MAGIC %run ./display_lib

# COMMAND ----------

# MAGIC %md ## Load Videos

# COMMAND ----------

# MAGIC %fs ls /mnt/raela/cctvVideos/train/

# COMMAND ----------

displayVid("/mnt/raela/cctvVideos/mp4/train/Browse2.mp4")

# COMMAND ----------

# MAGIC %md ## Process Videos - Extract Video Frames

# COMMAND ----------

import cv2
import uuid
import re

## Extract one video frame per second and save frame as JPG
def extractImages(pathIn):
    count = 0
    p = re.compile("/dbfs/mnt/raela/cctvVideos/train/(.*).mpg")
    vidName = str(p.search(pathIn).group(1))
    vidcap = cv2.VideoCapture(pathIn)
    success,image = vidcap.read()
    success = True
    while success:
      vidcap.set(cv2.CAP_PROP_POS_MSEC,(count*1000))
      success,image = vidcap.read()
      print ('Read a new frame: ', success)
      cv2.imwrite( "/dbfs/mnt/raela/cctvFrames_train/" + vidName + "frame%04d.jpg" % count, image)     # save frame as JPEG file
      count = count + 1
      print ('Wrote a new frame')
      
## Extract frames from all videos and save in dbfs folder
def createFUSEpaths(dbfsFilePath):
  return "/dbfs/" + dbfsFilePath[0][6:]

fileList = dbutils.fs.ls("/mnt/raela/cctvVideos/train/")
FUSEfileList = map(createFUSEpaths, fileList)
FUSEfileList_rdd = sc.parallelize(FUSEfileList)
FUSEfileList_rdd.map(extractImages).count()
display(dbutils.fs.ls("/mnt/raela/cctvFrames_train"))

# COMMAND ----------

# Remove files with 0 filesize

import os

def removeNulls(fileinfo):
  FUSEpath = "/dbfs/" + fileinfo[0][6:]
  if fileinfo.size == 0:
    os.remove(FUSEpath)
    return "removed ", fileinfo
  return "filesize OK"

map(removeNulls, dbutils.fs.ls("/mnt/raela/cctvFrames_train"))

# COMMAND ----------

# MAGIC %md ## Load images with Spark Deep Learning Pipelines 

# COMMAND ----------

import sparkdl

images = sparkdl.readImages("/mnt/raela/cctvFrames_train/", numPartition=32)
displayML(images)

# COMMAND ----------

# MAGIC %md ## Feature Extraction - DeepImageFeaturizer

# COMMAND ----------

from sparkdl import DeepImageFeaturizer

featurizer = DeepImageFeaturizer(inputCol="image", 
                                 outputCol="features", 
                                 modelName="InceptionV3")

features = featurizer.transform(images)

# COMMAND ----------

# This might take a few minutes
features.select("filePath", "features").coalesce(2).write.mode("overwrite").parquet("/mnt/raela/cctv_features2")

# COMMAND ----------

