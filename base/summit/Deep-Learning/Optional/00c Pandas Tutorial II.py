# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC We are going to run all of the cells in the previous notebook to get our variables defined in this notebook.

# COMMAND ----------

# MAGIC %run "./00b Pandas Tutorial I"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing Data
# MAGIC 
# MAGIC pandas primarily uses the value np.nan to represent missing data. It is by default not included in computations. See the [Missing Data section](https://pandas.pydata.org/pandas-docs/version/0.23.0/missing_data.html#missing-data)
# MAGIC 
# MAGIC Reindexing allows you to change/add/delete the index on a specified axis. This returns a copy of the data.

# COMMAND ----------

df1 = df.reindex(index=dates[0:4], columns=list(df.columns) + ['E'])
df1.loc[dates[0]:dates[1],'E'] = 1
df1

# COMMAND ----------

# MAGIC %md
# MAGIC To drop any rows that have missing data.

# COMMAND ----------

df1.dropna(how='any')

# COMMAND ----------

# MAGIC %md
# MAGIC Filling missing data

# COMMAND ----------

df1.fillna(value=5)

# COMMAND ----------

# MAGIC %md
# MAGIC To get the boolean mask where values are nan (note: had to change `isna` to `isnull` because our version of pandas is different).

# COMMAND ----------

pd.isnull(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operations
# MAGIC 
# MAGIC See the Basic section on [Binary Ops](https://pandas.pydata.org/pandas-docs/version/0.23.0/basics.html#basics-binop)
# MAGIC 
# MAGIC Operations in general exclude missing data.

# COMMAND ----------

# MAGIC %md
# MAGIC Performing a descriptive statistic

# COMMAND ----------

df.mean()

# COMMAND ----------

# MAGIC %md
# MAGIC Same operation on the other axis

# COMMAND ----------

df.mean(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Operating with objects that have different dimensionality and need alignment. In addition, pandas automatically broadcasts along the specified dimension.

# COMMAND ----------

s = pd.Series([1,3,5,np.nan,6,8], index=dates).shift(2)
s

# COMMAND ----------

df.sub(s, axis='index')

# COMMAND ----------

# MAGIC %md
# MAGIC Applying functions to the data

# COMMAND ----------

df.apply(np.cumsum)

# COMMAND ----------

df.apply(lambda x: x.max() - x.min())

# COMMAND ----------

# MAGIC %md
# MAGIC Histogramming
# MAGIC 
# MAGIC See more at [Histogramming and Discretization](https://pandas.pydata.org/pandas-docs/version/0.23.0/basics.html#basics-discretization)

# COMMAND ----------

s = pd.Series(np.random.randint(0, 7, size=10))
s

# COMMAND ----------

s.value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC String Methods
# MAGIC 
# MAGIC Series is equipped with a set of string processing methods in the str attribute that make it easy to operate on each element of the array, as in the code snippet below. Note that pattern-matching in str generally uses [regular expressions](https://docs.python.org/3/library/re.html) by default (and in some cases always uses them). See more at [Vectorized String Methods](https://pandas.pydata.org/pandas-docs/version/0.23.0/text.html#text-string-methods).

# COMMAND ----------

s = pd.Series(['A', 'B', 'C', 'Aaba', 'Baca', np.nan, 'CABA', 'dog', 'cat'])
s.str.lower()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge
# MAGIC 
# MAGIC pandas provides various facilities for easily combining together Series, DataFrame, and Panel objects with various kinds of set logic for the indexes and relational algebra functionality in the case of join / merge-type operations.
# MAGIC 
# MAGIC See the [Merging section](https://pandas.pydata.org/pandas-docs/version/0.23.0/merging.html#merging)

# COMMAND ----------

# MAGIC %md
# MAGIC Concatenating pandas objects together with [concat()](https://pandas.pydata.org/pandas-docs/version/0.23.0/generated/pandas.concat.html#pandas.concat)

# COMMAND ----------

df = pd.DataFrame(np.random.randn(10, 4))
df

# COMMAND ----------

# break it into pieces
pieces = [df[:3], df[3:7], df[7:]]
pd.concat(pieces)

# COMMAND ----------

# MAGIC %md
# MAGIC Join
# MAGIC 
# MAGIC SQL style merges. See the [Database style joining](https://pandas.pydata.org/pandas-docs/version/0.23.0/merging.html#merging-join)

# COMMAND ----------

left = pd.DataFrame({'key': ['foo', 'foo'], 'lval': [1, 2]})
left

# COMMAND ----------

right = pd.DataFrame({'key': ['foo', 'foo'], 'rval': [4, 5]})
right

# COMMAND ----------

pd.merge(left, right, on='key')

# COMMAND ----------

# MAGIC %md
# MAGIC Append rows to a dataframe. See the [Appending](https://pandas.pydata.org/pandas-docs/version/0.23.0/merging.html#merging-concatenation) section.

# COMMAND ----------

df = pd.DataFrame(np.random.randn(8, 4), columns=['A','B','C','D'])
df

# COMMAND ----------

s = df.iloc[3]
df.append(s, ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouping
# MAGIC 
# MAGIC By “group by” we are referring to a process involving one or more of the following steps
# MAGIC 
# MAGIC * Splitting the data into groups based on some criteria
# MAGIC * Applying a function to each group independently
# MAGIC * Combining the results into a data structure
# MAGIC 
# MAGIC See the [Grouping section](https://pandas.pydata.org/pandas-docs/version/0.23.0/groupby.html#groupby)

# COMMAND ----------

df = pd.DataFrame({'A' : ['foo', 'bar', 'foo', 'bar',
                          'foo', 'bar', 'foo', 'foo'],
                   'B' : ['one', 'one', 'two', 'three',
                          'two', 'two', 'one', 'three'],
                   'C' : np.random.randn(8),
                   'D' : np.random.randn(8)})

df

# COMMAND ----------

# MAGIC %md
# MAGIC Grouping and then applying the [sum()](https://pandas.pydata.org/pandas-docs/version/0.23.0/generated/pandas.DataFrame.sum.html#pandas.DataFrame.sum) function to the resulting groups.

# COMMAND ----------

df.groupby('A').sum()

# COMMAND ----------

# MAGIC %md
# MAGIC Grouping by multiple columns forms a hierarchical index, and again we can apply the sum function.

# COMMAND ----------

df.groupby(['A','B']).sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reshaping
# MAGIC 
# MAGIC See the sections on [Hierarchical Indexing](https://pandas.pydata.org/pandas-docs/version/0.23.0/advanced.html#advanced-hierarchical) and [Reshaping](https://pandas.pydata.org/pandas-docs/version/0.23.0/reshaping.html#reshaping-stacking).

# COMMAND ----------

tuples = list(zip(*[['bar', 'bar', 'baz', 'baz',
                     'foo', 'foo', 'qux', 'qux'],
                    ['one', 'two', 'one', 'two',
                     'one', 'two', 'one', 'two']]))

index = pd.MultiIndex.from_tuples(tuples, names=['first', 'second'])
df = pd.DataFrame(np.random.randn(8, 2), index=index, columns=['A', 'B'])
df2 = df[:4]
df2

# COMMAND ----------

# MAGIC %md
# MAGIC The [stack()](https://pandas.pydata.org/pandas-docs/version/0.23.0/generated/pandas.DataFrame.stack.html#pandas.DataFrame.stack) method “compresses” a level in the DataFrame’s columns.

# COMMAND ----------

stacked = df2.stack()
stacked

# COMMAND ----------

# MAGIC %md
# MAGIC With a “stacked” DataFrame or Series (having a MultiIndex as the index), the inverse operation of [stack()](https://pandas.pydata.org/pandas-docs/version/0.23.0/generated/pandas.DataFrame.stack.html#pandas.DataFrame.stack) is [unstack()](https://pandas.pydata.org/pandas-docs/version/0.23.0/generated/pandas.DataFrame.unstack.html#pandas.DataFrame.unstack), which by default unstacks the last level:

# COMMAND ----------

stacked.unstack()

# COMMAND ----------

stacked.unstack(1)

# COMMAND ----------

stacked.unstack(0)

# COMMAND ----------

# MAGIC %md
# MAGIC See the section on [Pivot Tables](https://pandas.pydata.org/pandas-docs/version/0.23.0/reshaping.html#reshaping-pivot).

# COMMAND ----------

df = pd.DataFrame({'A' : ['one', 'one', 'two', 'three'] * 3,
                   'B' : ['A', 'B', 'C'] * 4,
                   'C' : ['foo', 'foo', 'foo', 'bar', 'bar', 'bar'] * 2,
                   'D' : np.random.randn(12),
                   'E' : np.random.randn(12)})

df

# COMMAND ----------

# MAGIC %md
# MAGIC We can produce pivot tables from this data very easily:

# COMMAND ----------

pd.pivot_table(df, values='D', index=['A', 'B'], columns=['C'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Series
# MAGIC 
# MAGIC pandas has simple, powerful, and efficient functionality for performing resampling operations during frequency conversion (e.g., converting secondly data into 5-minutely data). This is extremely common in, but not limited to, financial applications. See the [Time Series section](https://pandas.pydata.org/pandas-docs/version/0.23.0/timeseries.html#timeseries).

# COMMAND ----------

rng = pd.date_range('1/1/2012', periods=100, freq='S')
ts = pd.Series(np.random.randint(0, 500, len(rng)), index=rng)
ts.resample('5Min').sum()

# COMMAND ----------

# MAGIC %md
# MAGIC Time zone representation:

# COMMAND ----------

rng = pd.date_range('3/6/2012 00:00', periods=5, freq='D')
ts = pd.Series(np.random.randn(len(rng)), rng)
ts

# COMMAND ----------

ts_utc = ts.tz_localize('UTC')
ts_utc

# COMMAND ----------

# MAGIC %md
# MAGIC Converting to another time zone:

# COMMAND ----------

ts_utc.tz_convert('US/Eastern')

# COMMAND ----------

# MAGIC %md
# MAGIC Converting between time span representations:

# COMMAND ----------

rng = pd.date_range('1/1/2012', periods=5, freq='M')
ts = pd.Series(np.random.randn(len(rng)), index=rng)
ts

# COMMAND ----------

ps = ts.to_period()
ps

# COMMAND ----------

ps.to_timestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC Converting between period and timestamp enables some convenient arithmetic functions to be used. In the following example, we convert a quarterly frequency with year ending in November to 9am of the end of the month following the quarter end:

# COMMAND ----------

prng = pd.period_range('1990Q1', '2000Q4', freq='Q-NOV')
ts = pd.Series(np.random.randn(len(prng)), prng)
ts.index = (prng.asfreq('M', 'e') + 1).asfreq('H', 's') + 9
ts.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Categoricals
# MAGIC 
# MAGIC pandas can include categorical data in a DataFrame. For full docs, see the [categorical introduction](https://pandas.pydata.org/pandas-docs/version/0.23.0/categorical.html#categorical) and the [API documentation](https://pandas.pydata.org/pandas-docs/version/0.23.0/api.html#api-categorical).

# COMMAND ----------

df = pd.DataFrame({"id":[1,2,3,4,5,6], "raw_grade":['a', 'b', 'b', 'a', 'a', 'e']})

# COMMAND ----------

# MAGIC %md
# MAGIC Convert the raw grades to a categorical data type.

# COMMAND ----------

df["grade"] = df["raw_grade"].astype("category")
df["grade"]

# COMMAND ----------

# MAGIC %md
# MAGIC Rename the categories to more meaningful names (assigning to Series.cat.categories is inplace!).

# COMMAND ----------

df["grade"].cat.categories = ["very good", "good", "very bad"]

# COMMAND ----------

# MAGIC %md
# MAGIC Reorder the categories and simultaneously add the missing categories (methods under Series .cat return a new Series by default).

# COMMAND ----------

df["grade"] = df["grade"].cat.set_categories(["very bad", "bad", "medium", "good", "very good"])
df["grade"]

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting is per order in the categories, not lexical order.

# COMMAND ----------

df.sort_values(by="grade")

# COMMAND ----------

# MAGIC %md
# MAGIC Grouping by a categorical column also shows empty categories.

# COMMAND ----------

df.groupby("grade").size()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plotting
# MAGIC 
# MAGIC See the [Plotting](https://pandas.pydata.org/pandas-docs/version/0.23.0/visualization.html#visualization) docs.

# COMMAND ----------

ts = pd.Series(np.random.randn(1000), index=pd.date_range('1/1/2000', periods=1000))
ts = ts.cumsum()
fig = ts.plot()
display(fig.figure) # Need to wrap in display for Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC On a DataFrame, the [plot()](https://pandas.pydata.org/pandas-docs/version/0.23.0/generated/pandas.DataFrame.plot.html#pandas.DataFrame.plot) method is a convenience to plot all of the columns with labels:

# COMMAND ----------

df = pd.DataFrame(np.random.randn(1000, 4), index=ts.index, columns=['A', 'B', 'C', 'D'])
df = df.cumsum()
plt.figure(); df.plot(); plt.legend(loc='best')
display(plt.show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Data In/Out

# COMMAND ----------

# MAGIC %md
# MAGIC [Writing to a csv file.](https://pandas.pydata.org/pandas-docs/version/0.23.0/io.html#io-store-in-csv)

# COMMAND ----------

df.to_csv('foo.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC [Reading from a csv file.](https://pandas.pydata.org/pandas-docs/version/0.23.0/io.html#io-read-csv-table)

# COMMAND ----------

pd.read_csv('foo.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC HDF5
# MAGIC 
# MAGIC Reading and writing to [HDFStores](https://pandas.pydata.org/pandas-docs/stable/io.html#io-hdf5).
# MAGIC 
# MAGIC Writing to a HDF5 Store.

# COMMAND ----------

# MAGIC %sh /databricks/python/bin/pip install tables
# MAGIC /databricks/python/bin/pip install xlrd

# COMMAND ----------

df.to_hdf('foo.h5','df')

# COMMAND ----------

# MAGIC %md
# MAGIC Reading from a HDF5 Store.

# COMMAND ----------

pd.read_hdf('foo.h5','df')

# COMMAND ----------

# MAGIC %md
# MAGIC Excel
# MAGIC 
# MAGIC Reading and writing to [MS Excel](https://pandas.pydata.org/pandas-docs/version/0.23.0/io.html#io-excel).
# MAGIC 
# MAGIC Writing to an excel file.

# COMMAND ----------

df.to_excel('foo.xlsx', sheet_name='Sheet1')

# COMMAND ----------

# MAGIC %md
# MAGIC Reading from an excel file.

# COMMAND ----------

pd.read_excel('foo.xlsx', 'Sheet1', index_col=None, na_values=['NA'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gotchas
# MAGIC 
# MAGIC If you are attempting to perform an operation you might see an exception like:

# COMMAND ----------

try:
  if pd.Series([False, True, False]):
      print("I was true")
      
except ValueError as e: 
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC See [Comparisons](https://pandas.pydata.org/pandas-docs/version/0.23.0/basics.html#basics-compare) for an explanation and what to do.
# MAGIC 
# MAGIC See [Gotchas](https://pandas.pydata.org/pandas-docs/version/0.23.0/gotchas.html#gotchas) as well.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>