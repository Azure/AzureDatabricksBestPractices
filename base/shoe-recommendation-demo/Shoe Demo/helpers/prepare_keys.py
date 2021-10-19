# Databricks notebook source
# MAGIC %run ./display_lib

# COMMAND ----------

# MAGIC %md ##### Data load

# COMMAND ----------

# The original parquet files for the demo are in the databricks-mllib s3 bucket. They have been copied to /dbfs on demo-shard for easier access. Here is how it was done for the record.

# img_arr_features_file = "s3a://databricks-mllib/demo-shoes/data/img-arrays-only-women-shoes.parquet" # image, img_array - a little faster to load
# features_file = "s3a://databricks-mllib/demo-shoes/data/features-women-shoes.parquet"                    # image, features
# sample_rows_file = "s3a://databricks-mllib/demo-shoes/data/sample-rows-women-shoes.parquet" # image, img_array, features

# dbutils.fs.mkdirs("dbfs:/shoes-demo/")
# dbutils.fs.cp(img_arr_features_file, "dbfs:/shoes-demo/img-arrays-only-women-shoes.parquet", True)
# dbutils.fs.cp(features_file, "dbfs:/shoes-demo/features-women-shoes.parquet", True)
# dbutils.fs.cp(sample_rows_file, "dbfs:/shoes-demo/sample-rows-women-shoes.parquet", True)
# display(dbutils.fs.ls("dbfs:/shoes-demo/"))

# COMMAND ----------

img_arr_features_file = "wasbs://shoe-demo@azuredbretaildemo.blob.core.windows.net/image-arrays"  # image, img_array
features_file = "wasbs://shoe-demo@azuredbretaildemo.blob.core.windows.net/features"                 # image, features
sample_rows_file = "wasbs://shoe-demo@azuredbretaildemo.blob.core.windows.net/sample_images"           # image, img_array, features

# COMMAND ----------



# COMMAND ----------

spark.conf.set(
  "fs.azure.sas.Demo.ygdemoseastus2.blob.core.windows.net",
  "BlobEndpoint=https://ygdemoseastus2.blob.core.windows.net/;TableEndpoint=https://ygdemoseastus2.table.core.windows.net/;SharedAccessSignature=sv=2017-11-09&ss=b&srt=sco&sp=rwdlac&se=2018-06-19T05:14:40Z&st=2018-06-18T21:14:40Z&spr=https&sig=6B%2FW%2BIQyxPr8Gz5tMCExFroA0lZU9A57Q4OV5ZIhofI%3D")

# COMMAND ----------

# MAGIC %fs cp -r wasbs://shoe-demo@azuredbretaildemo.blob.core.windows.net/image-arrays /FileStore/ShoeDemo/Img

# COMMAND ----------

# MAGIC %fs cp -r wasbs://shoe-demo@azuredbretaildemo.blob.core.windows.net/features /FileStore/ShoeDemo/Features

# COMMAND ----------

# MAGIC %fs cp -r wasbs://shoe-demo@azuredbretaildemo.blob.core.windows.net/sample_images /FileStore/ShoeDemo/Sample
# MAGIC   
# MAGIC   wasbs://Demo@ygdemoseastus2.blob.core.windows.net/sample_images
# MAGIC   

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.azuredbretaildemo.blob.core.windows.net",
  "HkniIkeb854+QQDnTMYxlJ13UwYozzAfEMkPrVZNL0QM3VpXn8CVqPn2dw8u2z1q0OyM7UUMeMzb6bzqV8ycbQ==")

# COMMAND ----------

#https://ygdemoseastus2.blob.core.windows.net/
  
#  6B%2FW%2BIQyxPr8Gz5tMCExFroA0lZU9A57Q4OV5ZIhofI%3D

spark.conf.set(
  "fs.azure.account.key.ygdemoseastus2.blob.core.windows.net",
  "https://ygdemoseastus2.blob.core.windows.net/;TableEndpoint=https://ygdemoseastus2.table.core.windows.net/;SharedAccessSignature=sv=2017-11-09&ss=b&srt=sco&sp=rwdlac&se=2018-06-19T05:14:40Z&st=2018-06-18T21:14:40Z&spr=https&sig=6B%2FW%2BIQyxPr8Gz5tMCExFroA0lZU9A57Q4OV5ZIhofI%3D")

# COMMAND ----------

# MAGIC %fs mkdirs wasbs://Demo@ygdemoseastus2.blob.core.windows.net/Sample

# COMMAND ----------

# MAGIC %fs ls /FileStore/ShoeDemo/Features2

# COMMAND ----------

image_data = sqlContext.read.parquet(img_arr_features_file).cache()
#image_data.write.parquet("/FileStore/ShoeDemo/Img2")
image_data.show()

# COMMAND ----------

90# load the raw image vector dataframe (4 min w/ 4 workers)
# might want to repartition with e.g. .repartition(256) for your cluster set-up 
# if you plan to run the featurization step live.
image_data = sqlContext.read.parquet(img_arr_features_file).cache()
#image_data.count()

# COMMAND ----------

#image_data.count()

# COMMAND ----------

#sample_images = sqlContext.read.parquet(sample_rows_file).cache()
#sample_images.write.parquet("/FileStore/ShoeDemo/Sample")
#sample_images.count()
sample_images.write.parquet("wasbs://Demo@ygdemoseastus2.blob.core.windows.net/sample_images")

# COMMAND ----------



# COMMAND ----------

# load featurized dataframe (1.74 min w/ 4 workers; 30 seconds w/ 16 workers)
features = sqlContext.read.parquet(features_file).cache()
features.write.parquet("/FileStore/ShoeDemo/Features2")
#features.count()

# COMMAND ----------

# MAGIC %md #####Some helper functions & variable prep to make the demo simpler

# COMMAND ----------

from pyspark.sql.functions import col
#sample_images_file = "dbfs:/sueann-demo/sample-women-shoes.parquet"
def get_sample_rows(df, n=5):
  sample_rows = sqlContext.read.parquet(sample_rows_file).orderBy('filePath')
  #sample_rows = df.limit(n-1).union(df.where(col('filePath') == 'dbfs:/sueann-demo/images/018118.jpg')) # black slides
  displayML(sample_rows.select("image", "filePath"))  
  return sample_rows
def find_feature_key(df, key_item):
  return df.filter(col('filePath') == key_item['filePath']).collect()[0]['features']

# COMMAND ----------

def displayTopK(nn):
  displayML(nn.select('image', 'distCol').orderBy('distCol'), 9)

# COMMAND ----------

# this is just to make the BucketedRandomProjectionLSH object creation look cleaner in the demo
#from pyspark.ml.feature import BucketedRandomProjectionLSH
#oldInit = BucketedRandomProjectionLSH.__init__
#def newInit(self, inputCol):
#  return oldInit(self, inputCol=inputCol,outputCol="hashes",bucketLength=2.0, numHashTables=3)
#BucketedRandomProjectionLSH.__init__ = newInit

# COMMAND ----------

sample_rows = get_sample_rows(image_data)
samples_collected = sample_rows.collect()
key_item = samples_collected[3]
arr_key = key_item["img_array"]
deep_key = find_feature_key(features, key_item)

# COMMAND ----------

deep_key2 = find_feature_key(features, samples_collected[0])
deep_key3 = find_feature_key(features, samples_collected[1])
deep_key4 = find_feature_key(features, samples_collected[2])
deep_key5 = find_feature_key(features, samples_collected[4])

# COMMAND ----------

