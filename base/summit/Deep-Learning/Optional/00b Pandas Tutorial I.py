# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intro to Pandas 
# MAGIC 
# MAGIC This notebook has copied the code from the [10 Minutes to pandas](https://pandas.pydata.org/pandas-docs/version/0.23.0/10min.html) for use in a Databricks notebook.
# MAGIC 
# MAGIC PLEASE do not upgrade the version of pandas in Databricks as it breaks the Databricks `display()` command.

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Object Creation
# MAGIC 
# MAGIC 
# MAGIC See the [Data Structure Intro section](https://pandas.pydata.org/pandas-docs/version/0.23.0/dsintro.html#dsintro)
# MAGIC 
# MAGIC Creating a [Series](https://pandas.pydata.org/pandas-docs/version/0.23.0/generated/pandas.Series.html#pandas.Series) by passing a list of values, letting pandas create a default integer index:

# COMMAND ----------

s = pd.Series([1,3,5,np.nan,6,8])
s

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a [DataFrame](https://pandas.pydata.org/pandas-docs/version/0.23.0/generated/pandas.DataFrame.html#pandas.DataFrame) by passing a numpy array, with a datetime index and labeled columns:

# COMMAND ----------

dates = pd.date_range('20130101', periods=6)
dates

# COMMAND ----------

df = pd.DataFrame(np.random.randn(6,4), index=dates, columns=list('ABCD'))
df

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a DataFrame by passing a dict of objects that can be converted to series-like.

# COMMAND ----------

df2 = pd.DataFrame({ 'A' : 1.,
                     'B' : pd.Timestamp('20130102'),
                     'C' : pd.Series(1,index=list(range(4)),dtype='float32'),
                     'D' : np.array([3] * 4,dtype='int32'),
                     'E' : pd.Categorical(["test","train","test","train"]),
                     'F' : 'foo' })

df2

# COMMAND ----------

# MAGIC %md
# MAGIC Having specific [dtypes](https://pandas.pydata.org/pandas-docs/version/0.23.0/basics.html#basics-dtypes)

# COMMAND ----------

df2.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Viewing Data
# MAGIC 
# MAGIC See the [Basics section](https://pandas.pydata.org/pandas-docs/version/0.23.0/basics.html#basics)
# MAGIC 
# MAGIC See the top & bottom rows of the frame

# COMMAND ----------

df.head()

# COMMAND ----------

df.tail(3)

# COMMAND ----------

# MAGIC %md
# MAGIC Display the index, columns, and the underlying numpy data

# COMMAND ----------

df.index

# COMMAND ----------

df.columns

# COMMAND ----------

df.values

# COMMAND ----------

# MAGIC %md
# MAGIC Describe shows a quick statistic summary of your data

# COMMAND ----------

df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC Transposing your data

# COMMAND ----------

df.T

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting by an axis

# COMMAND ----------

df.sort_index(axis=1, ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting by values

# COMMAND ----------

df.sort_values(by='B')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selection
# MAGIC 
# MAGIC Note While standard Python / Numpy expressions for selecting and setting are intuitive and come in handy for interactive work, for production code, we recommend the optimized pandas data access methods, .at, .iat, .loc, .iloc and .ix.
# MAGIC 
# MAGIC 
# MAGIC See the indexing documentation [Indexing and Selecting Data](https://pandas.pydata.org/pandas-docs/version/0.23.0/indexing.html#indexing) and [MultiIndex / Advanced Indexing](https://pandas.pydata.org/pandas-docs/version/0.23.0/advanced.html#advanced)

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting a single column, which yields a Series, equivalent to df.A

# COMMAND ----------

df['A']

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting via [], which slices the rows.

# COMMAND ----------

df[0:3]

# COMMAND ----------

df['20130102':'20130104']

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC See more in [Selection by Label](https://pandas.pydata.org/pandas-docs/version/0.23.0/indexing.html#indexing-label)
# MAGIC 
# MAGIC For getting a cross section using a label

# COMMAND ----------

df.loc[dates[0]]

# COMMAND ----------

df.loc[:,['A','B']]

# COMMAND ----------

# MAGIC %md
# MAGIC Showing label slicing, both endpoints are included

# COMMAND ----------

df.loc['20130102':'20130104',['A','B']]

# COMMAND ----------

# MAGIC %md
# MAGIC Reduction in the dimensions of the returned object

# COMMAND ----------

df.loc['20130102',['A','B']]

# COMMAND ----------

# MAGIC %md
# MAGIC For getting a scalar value

# COMMAND ----------

df.loc[dates[0],'A']

# COMMAND ----------

# MAGIC %md
# MAGIC For getting fast access to a scalar (equiv to the prior method)

# COMMAND ----------

df.at[dates[0],'A']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selection by Position
# MAGIC 
# MAGIC See more in [Selection by Position](https://pandas.pydata.org/pandas-docs/version/0.23.0/indexing.html#indexing-integer)
# MAGIC 
# MAGIC Select via the position of the passed integers

# COMMAND ----------

df.iloc[3]

# COMMAND ----------

# MAGIC %md
# MAGIC By integer slices, acting similar to numpy/python

# COMMAND ----------

df.iloc[3:5,0:2]

# COMMAND ----------

# MAGIC %md
# MAGIC By lists of integer position locations, similar to the numpy/python style

# COMMAND ----------

df.iloc[[1,2,4],[0,2]]

# COMMAND ----------

# MAGIC %md
# MAGIC For slicing rows explicitly

# COMMAND ----------

df.iloc[1:3,:]

# COMMAND ----------

# MAGIC %md
# MAGIC For slicing columns explicitly

# COMMAND ----------

df.iloc[:,1:3]

# COMMAND ----------

# MAGIC %md
# MAGIC For getting a value explicitly

# COMMAND ----------

df.iloc[1,1]

# COMMAND ----------

# MAGIC %md
# MAGIC For getting fast access to a scalar (equiv to the prior method)

# COMMAND ----------

df.iat[1,1]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Boolean Indexing
# MAGIC 
# MAGIC Using a single column’s values to select data.

# COMMAND ----------

df[df.A > 0]

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting values from a DataFrame where a boolean condition is met.

# COMMAND ----------

df[df > 0]

# COMMAND ----------

# MAGIC %md
# MAGIC Using the [isin()](https://pandas.pydata.org/pandas-docs/version/0.23.0/generated/pandas.Series.isin.html#pandas.Series.isin) method for filtering:

# COMMAND ----------

df2 = df.copy()
df2['E'] = ['one', 'one','two','three','four','three']
df2

# COMMAND ----------

df2[df2['E'].isin(['two','four'])]

# COMMAND ----------

# MAGIC %md
# MAGIC Setting a new column automatically aligns the data by the indexes

# COMMAND ----------

s1 = pd.Series([1,2,3,4,5,6], index=pd.date_range('20130102', periods=6))
s1

# COMMAND ----------

df['F'] = s1

# COMMAND ----------

# MAGIC %md
# MAGIC Setting values by label

# COMMAND ----------

df.at[dates[0],'A'] = 0

# COMMAND ----------

# MAGIC %md
# MAGIC Setting values by position

# COMMAND ----------

df.iat[0,1] = 0

# COMMAND ----------

# MAGIC %md
# MAGIC Setting by assigning with a numpy array

# COMMAND ----------

df.loc[:,'D'] = np.array([5] * len(df))

# COMMAND ----------

# MAGIC %md
# MAGIC The result of the prior setting operations

# COMMAND ----------

df

# COMMAND ----------

# MAGIC %md
# MAGIC A where operation with setting.

# COMMAND ----------

df2 = df.copy()
df2[df2 > 0] = -df2
df2

# COMMAND ----------

# MAGIC %md
# MAGIC We will continue in the [next notebook]($./00c Pandas Tutorial II)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>