# Databricks notebook source
# MAGIC %md NOTE - The import base path is relative to the current notebook, you cannot import something from a higher level than the current folder

# COMMAND ----------

import json
import base64
import sys
try:
  from urllib.parse import urlencode
  from urllib.request import Request, urlopen
except ImportError:
  from urllib import urlencode
  from urllib2 import Request, urlopen
from pyspark.sql import SparkSession

class DatabricksApiClient(object):
  def __init__(self):
    self.dbUtils = SparkSession.builder.getOrCreate().sparkContext._gateway.entry_point.getDbutils()
    self.HOST = str(self.dbUtils.notebook().getContext().apiUrl().get())
    self.TOKEN = str(self.dbUtils.notebook().getContext().apiToken().get())
    self.BASE_URL = '%s/api/2.0/workspace/' % (self.HOST)

  def list(self, basePath):
    query_string = urlencode({'path': basePath})
    request = Request(self.BASE_URL + 'list?{0}'.format(query_string))
    request.add_header("Authorization", "Bearer " + self.TOKEN)
    response = urlopen(request)
    if sys.version > '3':
      encoding = response.info().get_content_charset('utf-8')
      content = json.loads(response.read().decode(encoding))
    else:
      content = json.loads(response.read())

    return content['objects'] if 'objects' in content else []

  def getcontent(self, filePath):
    query_string = urlencode({'path': filePath, 'direct_download': 'true'})
    request = Request(self.BASE_URL + 'export?{0}'.format(query_string))
    request.add_header("Authorization", "Bearer " + self.TOKEN)
    response = urlopen(request)
    return response.read()

# COMMAND ----------

import io, os, sys, types, imp
import logging

log = logging.getLogger("NotebookImporter")
if len(log.handlers) == 0:
  ch = logging.StreamHandler(sys.stdout)
  ch.setLevel(logging.INFO)
  ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
  log.addHandler(ch)
  log.setLevel(logging.WARN)

# COMMAND ----------

class NotebookModuleLoader(object):
  def __init__(self, basePath):
    self.basePath = basePath
    self.sourceCache = {}
    self.apiClient = DatabricksApiClient()
    
  def get_code(self, fullname):
    filename = self.get_filename(fullname)
    src = self.apiClient.getcontent(filename)
    return compile(src, filename, 'exec')
  
  def get_data(self, path):
      pass
  
  def get_filename(self, fullname):
    return os.path.join(self.basePath, fullname.split('.')[-1])
  
  def load_module(self, fullname):
    log.info('Loading module %s from %s' % (fullname, self.basePath))
    
    code = self.get_code(fullname)
    mod = sys.modules.setdefault(fullname, imp.new_module(fullname))
    mod.__file__ = self.get_filename(fullname)
    mod.__loader__ = self
    pkg = fullname.rpartition('.')[0]
    mod.__package__ = pkg
    exec(code, mod.__dict__)
    return mod
  
  def is_package(self, fullname):
    return False

class NotebookPackageLoader(NotebookModuleLoader):
  def __init__(self, basePath):
    NotebookModuleLoader.__init__(self, basePath)

  def load_module(self, fullname):
    log.info('Loading package %s from %s' % (fullname, self.basePath))
    mod = NotebookModuleLoader.load_module(self, fullname)
    mod.__path__ = [ self.basePath ]
    mod.__package__ = fullname
    return mod

  def get_filename(self, fullname):
    return os.path.join(self.basePath, '__init__')

  def is_package(self, fullname):
    return True

# COMMAND ----------

class NotebookFinder(object):
  def __init__(self):
    dbUtils = SparkSession.builder.getOrCreate().sparkContext._gateway.entry_point.getDbutils()
    self.basePath = str(os.path.dirname(dbUtils.notebook().getContext().extraContext().get('notebook_path').get()))
    self.loaders = { self.basePath: NotebookModuleLoader(self.basePath) }
    self.apiClient = DatabricksApiClient()

  def __eq__(self, other):
    # just use name since if you re-run this notebook the actual type will be different
    return type(other).__name__ == type(self).__name__
  
  def find_module(self, fullname, path=None):
    log.info('Finding module %s under %s' % (fullname, path))
    
    if path is None:
      basePath = self.basePath
    elif len(path) > 0:
      if not path[0].startswith(self.basePath):
        return None
      basePath = path[0]      
    else:
      return None
    
    parts = fullname.split('.')
    basename = parts[-1]
    listing = []

    try:
      listing = self.apiClient.list(basePath)
    except AttributeError as err:
      log.error('Error fetching notebooks from %s: %s' % (basePath, err))
      pass

    found = [f for f in listing if f['path'] == os.path.join(basePath, basename) and f['object_type'] == 'DIRECTORY']

    if len(found) > 0:
      log.info('Found package %s' % (fullname))
      fullpath = os.path.join(basePath, basename)
      log.info('Using fullpath %s' % (fullpath))
      loader = NotebookPackageLoader(fullpath)
      try:
        loader.load_module(fullname)
        self.loaders[fullpath] = NotebookModuleLoader(fullpath)
        log.info('Package %s loaded' % (fullname))
      except ImportError as e:
        log.error('Error loading package', e)
        loader = None
      return loader

    found = [f for f in listing if f['path'] == os.path.join(basePath, basename) and f['object_type'] == 'NOTEBOOK']

    if len(found) > 0:
      log.info('Found module %s' % (fullname))
      if not basePath in self.loaders:
        self.loaders[basePath] = NotebookModuleLoader(basePath)
      return self.loaders[basePath]

    return None

# COMMAND ----------

if NotebookFinder() not in sys.meta_path:
  sys.meta_path.append(NotebookFinder())