# Databricks notebook source
# MAGIC %md 
# MAGIC #Imports

# COMMAND ----------

from azure.storage.blob import BlockBlobService
import os, sys
import pandas as pd
import math
import numpy as np
from pyspark.sql.types import StructType
from IPython.display import display
import datetime
import re

# COMMAND ----------

# MAGIC %md
# MAGIC #Connection to Blob Storage

# COMMAND ----------

#Allows us to connect to Azure Blob Storage #######################################
#Azure Blob Storage Name
storage_account_name = "devasaanalytica2"
#Azure Blob Storage Access Key
storage_account_access_key = "fcvBvFdqIhV5viN98jnaUhskBRRHY7jZXUGS/KOumT0Dtl3H3XyI/h2T9psBw/YH8G0FSBRK+ssr+LxwkJ0Kxg=="

# COMMAND ----------

strRootBlobName_SourceFolder = "landing"
wasbs_string_Source = "wasbs://" + strRootBlobName_SourceFolder + "@"+ storage_account_name +".blob.core.windows.net" 
mount = "/mnt/landing"
config_key = "fs.azure.account.key." + storage_account_name + ".blob.core.windows.net"

# COMMAND ----------

dbutils.fs.mount(
  source = wasbs_string_Source,
  mount_point = mount,
  extra_configs = {config_key:storage_account_access_key})

# COMMAND ----------

# MAGIC %md
# MAGIC #Utils Functions

# COMMAND ----------

def copyFiles(filename, destinationame):
  # Copy file filename to destination destinationname
  #
  # Logs the errors
  
  try:
    dbutils.fs.cp(filename, destinationame)
  except Exception as e:
    print('hello') #log error

# COMMAND ----------

def deleteFile(filename):
  # Delete file filename 
  #
  # Logs the errors
  
  try:
    dbutils.fs.rm(filename)
  except Exception as e:
    print('hello') #log error

# COMMAND ----------

def checkZipName(source_sys, zipName):
  # Check if zip file name in format YYYYmmddHHMMSS_JobNbr_syst.zip 
  #       with syst = source_sys
  # Returns a boolean value
  #
  # Logs errors and rejects

  zipNameCorrect = True  
  m = re.search('(\d{14})_(\d+)_(\w+).zip', zipName)
  if m is None:
    zipNameCorrect = False
    
  else:
    date_text= m.group(1)
    jobnbr   = m.group(2)
    system   = m.group(3)
    zipNameCorrect = ((system + '/') == source_sys)
    if zipNameCorrect:
      try:
        datetime.datetime.strptime(date_text, '%Y%m%d%H%M%S')
      except ValueError:
        zipNameCorrect = False
    
  return zipNameCorrect

# COMMAND ----------

def checkFileName(source_sys, fileName):
  # Check if zip file name in format YYYYMMDDHHMMSS_ExtractType_Vnnn_syst_EntityName_DATA.txt  
  #                                  YYYYMMDDHHMMSS_ExtractType_Vnnn_syst_EntityName_CTRL.txt
  #       with syst = source_sys
  # Returns a boolean value
  #
  # Logs errors and rejects
  
  fileNameCorrect = True
  m = re.search('(\d{14})_(\w+)_V(\d{3})_(\w+)_(\w+)_(\w{4}).txt', fileName)
  if m is None:
    fileNameCorrect = False
    
  else:
    date_text   = m.group(1) #
    extr_type   = m.group(2)
    #version_nbr = m.group(3)
    system      = m.group(4) #
    #entity_nm   = m.group(5)
    file_type   = m.group(6) #
  
    fileNameCorrect = ((system + '/') == source_sys) and ((file_type=='DATA') or (file_type=='CTRL'))
    
    if fileNameCorrect:
      try:
        datetime.datetime.strptime(date_text, '%Y%m%d%H%M%S')
      except ValueError:
        fileNameCorrect = False
  
  return fileNameCorrect

# COMMAND ----------

# MAGIC %md
# MAGIC #Main

# COMMAND ----------

print(checkZipName('MDM/','20181213131900_000100_MDM.zip'))

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /dbfs/mnt/landing/MDM/20181213131900_000100_MDM.zip -d /dbfs/mnt/landing/MDM/zip

# COMMAND ----------

landing_folderfiles = dbutils.fs.ls('/mnt/landing/MDM/zip')
for landing_file in landing_folderfiles:
  file_ok = checkFileName('MDM/',landing_file.name)
  if file_ok:
    print('Nice')
  else:
    print('KO')

# COMMAND ----------

# MAGIC %md 
# MAGIC #Brouillon

# COMMAND ----------

copyFiles('/mnt/landing/MDM/zip/20181115040751_F_V001_MDM_OrganizationType_CTRL.txt', '/mnt/landing/MDM/mv/20181115040751_F_V001_MDM_OrganizationType_CTRL.txt')

# COMMAND ----------

deleteFile('/mnt/landing/MDM/mv/20181115040751_F_V001_MDM_OrganizationType_CTRL.txt')

# COMMAND ----------

print(checkFileName('HSES/','20181115040751_F_V001_HSES_OrganizationType_DATA.txt'))

# COMMAND ----------

landing_foldernames = dbutils.fs.ls('/mnt/landing/')
for folder in landing_foldernames:
  sub_fold_filenames = dbutils.fs.ls(folder.path)
  for landing_file in sub_fold_filenames:
    print(checkZipName(folder.name, landing_file.name))

# COMMAND ----------

# MAGIC %sh
# MAGIC mypath = '/dbfs/mnt/landing/MDM/20181213131900_000100_MDM.zip'
# MAGIC unzip $mypath -d /dbfs/mnt/landing/MDM/zip

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /dbfs/mnt/landing/MDM/20181213131900_000100_MDM.zip -d /dbfs/mnt/landing/MDM/zip
# MAGIC unzip /dbfs/mnt/landing/MDM/20181217163800_000100_MDM.zip -d /dbfs/mnt/landing/MDM/zip

# COMMAND ----------

HSES_landing_filenames = dbutils.fs.ls('/mnt/landing/HSES')
print(HSES_zip_names)

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/landing/HSES/

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/landing/HSES/zip

# COMMAND ----------

dbutils.fs.unmount(mount)