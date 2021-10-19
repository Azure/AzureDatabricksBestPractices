# Databricks notebook source
from pyspark.sql.types import *

schema =StructType([
	StructField("GRCODE",IntegerType(), False),
	StructField("GRNAME",StringType(), False),
	StructField("AccidentYear",IntegerType(), False),
	StructField("DevelopmentYear",IntegerType(), False),
	StructField("DevelopmentLag",IntegerType(), False),
	StructField("IncurLoss_B",IntegerType(), False),
	StructField("CumPaidLoss_B",IntegerType(), False),
	StructField("BulkLoss_B",IntegerType(), False),
	StructField("EarnedPremDIR_B",IntegerType(), False),
	StructField("EarnedPremCeded_B",IntegerType(), False),
	StructField("EarnedPremNet_B",IntegerType(), False),
	StructField("Single",IntegerType(), False),
	StructField("PostedReserve97_B",IntegerType(), False)
])
    
df = (spark.read.option("delimiter", ",")
  .option("inferSchema", "true")  # Use tab delimiter (default is comma-separator)
   .option("header", "true")
  .schema(schema)
  .csv("dbfs:/mnt/claim/ppauto_pos.csv"))

# COMMAND ----------

display(df)

# COMMAND ----------

# Because we will need it later...
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# For each maxDepth setting, make predictions on the test data, and compute the accuracy metric.
accuracies_list = []
maxDepth = 11
for maxYear in range(1988,1998):
    tempdf = df.select("GRCODE","GRNAME","AccidentYear","DevelopmentYear","DevelopmentLag","IncurLoss_B","CumPaidLoss_B","BulkLoss_B","EarnedPremDIR_B","EarnedPremCeded_B","EarnedPremNet_B","Single","PostedReserve97_B").filter((col('DevelopmentLag') < maxDepth) & (col('AccidentYear') == maxYear) )
    accuracies_list.append(tempdf)
    maxDepth = maxDepth-1
    
def union_all(dfs):
    if len(dfs) > 1:
        return dfs[0].unionAll(union_all(dfs[1:]))
    else:
        return dfs[0]

traindf = union_all(accuracies_list)

# COMMAND ----------

# MAGIC %sql 
# MAGIC     
# MAGIC     CREATE TEMPORARY TABLE temp_idsdata
# MAGIC     USING parquet
# MAGIC     OPTIONS (
# MAGIC       path "/mnt/databricks-wesley/demo-data/insurance/lossdata1"
# MAGIC     )

# COMMAND ----------

importance = sqlContext.sql("select AccidentYear,DevelopmentLag,CumPaidLoss_B from temp_idsdata where GRCODE=43 order by AccidentYear,DevelopmentLag")
importanceDF = importance.toPandas()
pivotimpdf = importanceDF.pivot("DevelopmentLag", "AccidentYear", "CumPaidLoss_B")

# COMMAND ----------

print pivotimpdf

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
sns.set()    
plt.title("Loss Reserve")
ax = sns.heatmap(pivotimpdf, mask=pivotimpdf.isnull(),annot=True, fmt=".0f")
ax.set_xlabel("Accident Year")
ax.set_ylabel("Development Lag")
plt.xticks(rotation=12)
plt.tight_layout()
plt.grid(True)
plt.show()
display()

# COMMAND ----------

print np.random.randn(10, 10)

# COMMAND ----------

import numpy as np 
from pandas import DataFrame
import matplotlib.pyplot as plt

a = np.zeros((10, 10))
Index= ['1988', '1989', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997']
Cols = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
df = DataFrame(pivotimpdf, index=Index, columns=Cols)

plt.pcolor(pivotimpdf)
plt.yticks(np.arange(0.5, len(df.index), 1), df.index)
plt.xticks(np.arange(0.5, len(df.columns), 1), df.columns)
plt.grid(True)
plt.show()
display()

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

importance = sqlContext.sql("select CumPaidLoss_B from temp_idsdata where GRCODE=43 ")
importanceDF = importance.toPandas()
a = np.random.random((16, 15))
plt.imshow(a, cmap='hot', interpolation='nearest')
plt.show()
display()

# COMMAND ----------

# MAGIC %scala
# MAGIC val closs = sqlContext.read.format("csv")
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .load("/FileStore/tables/w9c0757b1502730977079/")
# MAGIC 
# MAGIC display(closs)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC val x = closs.sort(asc("AY")).select("AY").distinct.collect().mkString(",")
# MAGIC val y = closs.sort(desc("Dev")).select("Dev").distinct.collect().mkString(",")
# MAGIC val z = closs.select("valyear")
# MAGIC 
# MAGIC val toRemove = "[]".toSet
# MAGIC val xaxis = x.filterNot(toRemove)
# MAGIC val yaxis = y.filterNot(toRemove)

# COMMAND ----------

# MAGIC %scala
# MAGIC val rawCSV = closs.collect().mkString(",")

# COMMAND ----------

# MAGIC %scala
# MAGIC closs.printSchema

# COMMAND ----------

# MAGIC %scala
# MAGIC displayHTML(s"""
# MAGIC <head>
# MAGIC   <!-- Plotly.js -->
# MAGIC   <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
# MAGIC </head>
# MAGIC 
# MAGIC <body>
# MAGIC   
# MAGIC   <div id="myDiv"><!-- Plotly chart will be drawn inside this DIV --></div>
# MAGIC   <script>
# MAGIC var xValues = [${xaxis}];
# MAGIC 
# MAGIC var yValues = [${yaxis}];
# MAGIC 
# MAGIC var zValues = [
# MAGIC   [75000, 105000, 135000, 165000, 195000, 225000, 255000, 285000],
# MAGIC   [65000, 95000, 125000, 155000, 185000, 215000, 245000, 0],
# MAGIC   [55000, 85000, 105000, 145000, 175000, 205000, 0, 0],
# MAGIC   [45000, 75000, 105000, 135000, 165000, 0, 0, 0],
# MAGIC   [35000, 65000, 95000, 125000, 0, 0, 0, 0],
# MAGIC   [25000, 55000, 85000, 0, 0, 0, 0, 0],
# MAGIC   [15000, 45000, 0, 0, 0, 0, 0, 0],
# MAGIC   [5000, 0, 0, 0, 0, 0, 0, 0]
# MAGIC ];
# MAGIC 
# MAGIC var colorscaleValue = [
# MAGIC   [0, '#3D9970'],
# MAGIC   [1, '#001f3f']
# MAGIC ];
# MAGIC 
# MAGIC var data = [{
# MAGIC   x: xValues,
# MAGIC   y: yValues,
# MAGIC   z: zValues,
# MAGIC   type: 'heatmap',
# MAGIC   colorscale: colorscaleValue,
# MAGIC   showscale: false
# MAGIC }];
# MAGIC 
# MAGIC var layout = {
# MAGIC   title: 'Annotated Heatmap',
# MAGIC   annotations: [],
# MAGIC   xaxis: {
# MAGIC     ticks: '',
# MAGIC     side: 'top'
# MAGIC   },
# MAGIC   yaxis: {
# MAGIC     ticks: '',
# MAGIC     ticksuffix: ' ',
# MAGIC     width: 700,
# MAGIC     height: 700,
# MAGIC     autosize: false
# MAGIC   }
# MAGIC };
# MAGIC 
# MAGIC for ( var i = 0; i < yValues.length; i++ ) {
# MAGIC   for ( var j = 0; j < xValues.length; j++ ) {
# MAGIC     var currentValue = zValues[i][j];
# MAGIC     if (currentValue != 0.0) {
# MAGIC       var textColor = 'white';
# MAGIC     }else{
# MAGIC       var textColor = 'black';
# MAGIC     }
# MAGIC     var result = {
# MAGIC       xref: 'x1',
# MAGIC       yref: 'y1',
# MAGIC       x: xValues[j],
# MAGIC       y: yValues[i],
# MAGIC       text: zValues[i][j],
# MAGIC       font: {
# MAGIC         family: 'Arial',
# MAGIC         size: 12,
# MAGIC         color: 'rgb(50, 171, 96)'
# MAGIC       },
# MAGIC       showarrow: false,
# MAGIC       font: {
# MAGIC         color: textColor
# MAGIC       }
# MAGIC     };
# MAGIC     layout.annotations.push(result);
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC Plotly.newPlot('myDiv', data, layout);
# MAGIC   </script>
# MAGIC </body>
# MAGIC """)