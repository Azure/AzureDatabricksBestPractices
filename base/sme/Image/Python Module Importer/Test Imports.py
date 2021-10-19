# Databricks notebook source
# MAGIC %run ./InitWorkspaceImporter

# COMMAND ----------

# validate that regular imports still work...
import pandas as pd
import ggplot

# COMMAND ----------

import simplemodule

simplemodule.someFunction("Bazinga")

# COMMAND ----------

# open the simplemodule notebook, change logic, and reload on the fly!
import sys

if sys.version > '3':
  from importlib import reload

reload(simplemodule)

simplemodule.someFunction("Bazinga")

# COMMAND ----------

# import modules using __init__
from somepackage import *

my = mymodule1.MyClass()
my.foo()

# COMMAND ----------

myother = mymodule2.MyOtherClass()
myother.helloWorld()

# COMMAND ----------

# nested imports
myother.nestedImport()

# COMMAND ----------

