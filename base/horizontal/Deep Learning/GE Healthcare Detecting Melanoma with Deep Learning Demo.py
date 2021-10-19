# Databricks notebook source
# MAGIC %md #GE Healthcare Detecting Melanoma with Deep Learning
# MAGIC 
# MAGIC Melanoma is one of the mostly deadliest forms of skin cancer with over 75,000 cases in the US each year.
# MAGIC 
# MAGIC Melanoma is also hard to detect as not all skin moles and lesions are cancerous. 
# MAGIC 
# MAGIC This demo is based on the ISIC 2017: Skin Lesion Analysis Towards Melanoma Detection Contest Sponsored by the *International Skin Imaging Collaboration*
# MAGIC 
# MAGIC https://challenge.kitware.com/#challenge/583f126bcad3a51cc66c8d9a

# COMMAND ----------

# MAGIC %md ##1. Install TensorFlow onto the GPU-Enabled Spark Cluster

# COMMAND ----------

# The name of the cluster on which to install TensorFlow:
clusterName = "tf-gpu" mnji

# TensorFlow binary URL
tfBinaryUrl = "https://storage.googleapis.com/tensorflow/linux/gpu/tensorflow-0.11.0-cp27-none-linux_x86_64.whl"

script = """#!/usr/bin/env bash

set -ex

echo "**** Installing GPU-enabled TensorFlow *****"

pip install {tfBinaryUrl}
git clone -b r0.11 https://github.com/tensorflow/tensorflow.git
mkdir -p /databricks/spark/python/tensorflow
cp -prv tensorflow/tensorflow/examples /databricks/spark/python/tensorflow
/databricks/python/bin/pip install --upgrade https://storage.googleapis.com/tensorflow/linux/cpu/protobuf-3.2.0-cp27-none-linux_x86_64.whl
""".format(tfBinaryUrl = tfBinaryUrl)
dbutils.fs.rm("dbfs:/databricks/init/%s/install-tensorflow-gpu.sh" % clusterName)
dbutils.fs.mkdirs("dbfs:/databricks/init/")
dbutils.fs.put("dbfs:/databricks/init/%s/install-tensorflow-gpu.sh" % clusterName, script, True)

# COMMAND ----------

# MAGIC %md You can also install mxnet

# COMMAND ----------

mxnetGitTag = '2413a8e'
script = """#!/usr/bin/env bash

set -ex

echo "**** Installing MXNet dependencies ****"

apt-get update

# Requirements stated in http://mxnet.io/get_started/setup.html#standard-installation.
# We used OpenBLAS instead of ATLAS.
apt-get install -y build-essential git libopenblas-dev libopencv-dev python-numpy python-setuptools

echo "**** Downloading MXNet ****"

MXNET_HOME=/usr/local/mxnet
git clone --recursive https://github.com/dmlc/mxnet $MXNET_HOME
cd $MXNET_HOME
git checkout {mxnetGitTag}
git submodule update

echo "**** Building MXNet ****"

#USE_CUDA=1 USE_CUDNN=1 USE_CUDA_PATH=/usr/local/cuda USE_BLAS=openblas make -e -j$(nproc)
USE_CUDA=0 USE_CUDNN=0 USE_CUDA_PATH=/usr/local/cuda USE_BLAS=openblas make -e -j$(nproc)

echo "**** Installing MXNet ****"

cd python
python setup.py install

""".format(mxnetGitTag = mxnetGitTag)
dbutils.fs.rm("dbfs:/databricks/init/%s/install-mxnet-gpu.sh" % clusterName)
dbutils.fs.put("dbfs:/databricks/init/%s/install-mxnet-gpu.sh" % clusterName, script, True)

# COMMAND ----------

# MAGIC %md **RESTART THE CLUSTER TO INSTALL TENSORFLOW**
# MAGIC 
# MAGIC Check the library installed.

# COMMAND ----------

import tensorflow as tf

# COMMAND ----------

# MAGIC %md ##2. Load the Labels from a CSV File

# COMMAND ----------

# MAGIC %run ./data_Setup

# COMMAND ----------

labels = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/horizontal/imageprocessing/label.csv")

# COMMAND ----------

display(labels)

# COMMAND ----------

display(labels.groupBy("melanoma").count())

# COMMAND ----------

melanoma = labels.where("melanoma = 1")

# COMMAND ----------

benign = labels.where("melanoma = 0")

# COMMAND ----------

# MAGIC %md ## 3. Cache Data to the SSD

# COMMAND ----------

import os
import shutil

def cacheFilesAndReturn(images, subdir):
  file_dir = '/tmp/training/'+subdir+'/'
  try:
    os.makedirs(str(file_dir))
  except:
    pass
  for image_id in images:
    if os.path.exists("/dbfs/mnt/wesley/dataset/horizontal/imageprocessing/image/training/%s.jpg" % image_id):
      shutil.copyfile("/dbfs/mnt/wesley/dataset/horizontal/imageprocessing/image/training/%s.jpg" % image_id, str(file_dir)+"%s.jpg" % image_id)

# COMMAND ----------

cacheFilesAndReturn(melanoma.select("image_id").rdd.map(lambda x: x[0]).collect(), "melanoma")

# COMMAND ----------

cacheFilesAndReturn(benign.select("image_id").rdd.map(lambda x: x[0]).collect(), "benign")

# COMMAND ----------

# MAGIC %md ##4. Explore the Dataset

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.image as img
import mxnet

# COMMAND ----------

melanomaImg = "/tmp/training/melanoma/" + os.listdir("/tmp/training/melanoma/")[0]
benignImg = "/tmp/training/benign/" + os.listdir("/tmp/training/benign/")[0]
print melanomaImg
print benignImg

# COMMAND ----------

plt.imshow(mxnet.image.imdecode(open(melanomaImg).read()).asnumpy())
plt.title("Melanoma")
display(plt.show())

# COMMAND ----------

plt.imshow(mxnet.image.imdecode(open(benignImg).read()).asnumpy())
plt.title("Benign")
display(plt.show())

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ##5.Train the Model using a Convolution Neural Network

# COMMAND ----------

# MAGIC %sh /databricks/python/bin/python -u /databricks/spark/python/tensorflow/examples/image_retraining/retrain.py --image_dir "/tmp/training"  --output_graph "/tmp/melanoma.pb"

# COMMAND ----------

dbutils.fs.cp("file:/tmp/melanoma.pb", "dbfs:/melanoma/melanoma.pb", True)

# COMMAND ----------

dbutils.fs.cp('file:/tmp/retrain_logs', 'dbfs:/melanoma/', True)

# COMMAND ----------

# MAGIC %md %md ##5.Scoring Images using a Convolution Neural Network

# COMMAND ----------

dbutils.fs.mkdirs("file:/tmp/retrain_logs")

# COMMAND ----------

dbutils.fs.cp('dbfs:/melanoma/train', 'file:/tmp/retrain_logs', True)

# COMMAND ----------

dbutils.fs.cp('dbfs:/melanoma/melanoma.pb', 'file:/tmp/melanoma.pb', True)

# COMMAND ----------

with tf.gfile.FastGFile("/tmp/melanoma.pb", 'rb') as f:
    graph_def = tf.GraphDef()
    graph_def.ParseFromString(f.read())
    _ = tf.import_graph_def(graph_def, name='')

# COMMAND ----------

def displayPrediction(img_path, label):
  image_data = tf.gfile.FastGFile(img_path, 'rb').read()
  with tf.Session() as sess:
    # Feed the image_data as input to the graph and get first prediction
    softmax_tensor = sess.graph.get_tensor_by_name('final_result:0')
    
    predictions = sess.run(softmax_tensor, \
             {'DecodeJpeg/contents:0': image_data})
    
    # Sort to show labels of first prediction in order of confidence
    #top_k = predictions[0].argsort()[-len(predictions[0]):][::-1]
    plt.imshow(mxnet.image.imdecode(open(img_path).read()).asnumpy())
    plt.title(label)
    plt.figtext(0,0,'Model Prediction: Not Cancer: %.5f, Cancer: %.5f' % (predictions[0][0], predictions[0][1]))
    display(plt.show())
    plt.close()

# COMMAND ----------

displayPrediction(melanomaImg, "Melanoma")

# COMMAND ----------

displayPrediction(benignImg, "Beningn")

# COMMAND ----------

# MAGIC %sh tensorboard --host 0.0.0.0  --logdir /tmp/retrain_logs

# COMMAND ----------

# MAGIC %scala
# MAGIC val publicKey = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDWtUsDZ01/6eDITqtMxO3Y6qHrKhWSU76/0LB/5AV4v6pIeK86QxZGstDCVMO4XlEoLvwVlWiOWsSQkyGJYFCHp0xOZUD3LTkF0WWb6NcOObiSnjmCxc70mhknEoOqqu9Q+C0IGaedr/VOnQj0VYJbOvrTb1pUOJY3zbv3ZAGnlLKyXc4mnxINoFueVR6nZdHxH4cFxsC4wx/qVNJATrjrxSQIvXRBq+aYIy117He/GFljqGbHmo4VRDA2QBtBZoWiNOTVbGaDKsZjgf+BJwOxjugmrm4x74NN+Dazx60JkTV4/1XFKVvBShCEMTQSvY0pu47t1vPMoh2Ip22FbqkqB0DM0i+BfBL1sClGArtWRquNJbxKV58doQjXaNCIkrocb0AHx9rm+F0Qn6mFdrtR4p5u2X0U8YlBl3j0/gVvIRXmn16S8DOvtEYleoA58YBHWcryyft3VHxAf9AxzD9F8WgS+32iHTOfixse8ihWhTJCFajnK60XKhmC0LNFEs7DBzyXNm5D3Ys0I8zRUxDCgjIQFzhs3VjzjdJEb8jqxryFYPXMXaI4BT2jynVpaXlY88csf6fz94XwqEKDVq+F1g2Z1Dc5rZYtQMPfyryMWbmpOfmzxXP14Pv8lAZs5wNy9vMW4oUu074ZhKD9CJw8+uCRZDD3Lbjgh0aid7f+HQ== rlgarris@databricks.com"
# MAGIC 
# MAGIC def addAuthorizedPublicKey(key: String): Unit = {
# MAGIC         val fw = new java.io.FileWriter("/home/ubuntu/.ssh/authorized_keys", true)
# MAGIC         fw.write("\n" + key)
# MAGIC         fw.close()
# MAGIC }
# MAGIC addAuthorizedPublicKey(publicKey)

# COMMAND ----------

# MAGIC %md Command ssh -i id_rsa -p 2200 -l ubuntu  ec2-35-166-147-71.us-west-2.compute.amazonaws.com

# COMMAND ----------

