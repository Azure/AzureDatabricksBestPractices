# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC * [**Demand Forecast**](https://en.wikipedia.org/wiki/Demand_forecasting) is the art and science of forecasting customer demand to drive holistic execution of such demand by corporate supply chain and business management and is...  
# MAGIC   * Built on top of Apache Spark in and written in Scala inside Databricks Platform
# MAGIC   * Uses a machine learning **Random Forest** implementation to find the most *important* attribute to a demand forecast of interest   
# MAGIC * This demo...  
# MAGIC   * Includes a dataset with a subset of simulated for store transaction

# COMMAND ----------

# MAGIC %md
# MAGIC ###0. SETUP -- Databricks Spark cluster:  
# MAGIC 
# MAGIC 1. **Create** a cluster by...  
# MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` OR Click [Here](https://demo.cloud.databricks.com/#clusters/create) 
# MAGIC   - Enter any text, i.e `demo` into the cluster name text box
# MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
# MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
# MAGIC 3. **Attach** this notebook to your cluster by...   
# MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step1: Ingest Demand Forecast Data
# MAGIC * The demad forecast data consist of 4 generated datasets. They are menu,sales,transaction and location.
# MAGIC * Runs the "data_Setup" Notebook to populate the temp table to be used in this notebook. This notebook is kept under the same folder as this notebook.

# COMMAND ----------

# MAGIC %run ./data_Setup

# COMMAND ----------

# MAGIC %md we can query the `menu` Dataframe using Spark SQL - just type `%sql`
# MAGIC 
# MAGIC `%sql` is a magic tag that tells the notebook to use another programming language
# MAGIC We also support `%scala`, `%r`, `%python`, `%fs`, and `%sh`

# COMMAND ----------

# MAGIC %sql select name from menu

# COMMAND ----------

# MAGIC %md ### Step2: Enrich the data by joining the datasets to get complete view of the sales transaction 
# MAGIC - Joined the dataset for transaction,location and menu to give a snapshot view of the transaction at any given time

# COMMAND ----------

joinedData = spark.sql("""
  select t.timestamp, t.transaction, t.DATE, 
  l.locationid, l.name as location_name, l.address, l.city, l.state, l.zipcode, l.latitude, l.longitude,
  m.SKU, m.Name as menu_name 
  from transactions t
    join locations l
      on t.locationid = l.locationid
    join menu m
      on t.SKU = m.SKU
 """)

# COMMAND ----------

# MAGIC %md Persist the results to Disk

# COMMAND ----------

#joinedData.write.partitionBy("DATE").parquet('/mnt/wesley/parquet/demandforecast/sales_demo')

# COMMAND ----------

# MAGIC %md ###Step3: Explore demand forecast Data by capturing transaction into a Table and Analyze Data

# COMMAND ----------

# DBTITLE 1,Store locations around the United States
# MAGIC %sql select state, count(*) from locations group by state order by count(*)

# COMMAND ----------

# DBTITLE 1,Demand By Store
# MAGIC %scala
# MAGIC 
# MAGIC import com.esri.core.geometry._
# MAGIC 
# MAGIC def isWithinBounds(point: Seq[Double], bounds: Seq[Seq[Double]]): Boolean = {
# MAGIC   val _point = new Point(point(0), point(1))
# MAGIC   
# MAGIC   val _bounds = new Polygon()
# MAGIC   _bounds.startPath(bounds(0)(0), bounds(0)(1))
# MAGIC   _bounds.lineTo(bounds(1)(0), bounds(1)(1))
# MAGIC   _bounds.lineTo(bounds(2)(0), bounds(2)(1))
# MAGIC   _bounds.lineTo(bounds(3)(0), bounds(3)(1))
# MAGIC   
# MAGIC   OperatorWithin.local().execute(_point, _bounds, SpatialReference.create("WGS84"), null)
# MAGIC }
# MAGIC 
# MAGIC val isWithin = udf(isWithinBounds(_: Seq[Double], _: Seq[Seq[Double]]))
# MAGIC 
# MAGIC import org.apache.spark.sql.expressions._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val boundingBox1 = ("NYC", Array(Array(-74.431, 40.293), Array(-74.431, 41.286), Array(-72.276, 40.293), Array(-72.276, 41.286)))
# MAGIC val boundingBox2 = ("Baltimore/DC", Array(Array(-76.686, 38.335), Array(-76.686, 39.47), Array(-76.0, 38.335), Array(-76.0, 39.47)))
# MAGIC 
# MAGIC val bounds = sc.parallelize(Seq(boundingBox1, boundingBox2)).toDF("name", "bounds")
# MAGIC 
# MAGIC val boundingBox = bounds.filter($"name" === "NYC").select($"bounds").collect().head.getAs[Seq[Seq[Double]]](0)
# MAGIC 
# MAGIC val withVoyage2 = spark.sql(s""" select locationid, longitude, latitude, count(*) as weight from sales group by locationid, longitude, latitude """)
# MAGIC val points = withVoyage2.select($"longitude".as("lng"), $"latitude".as("lat")).toJSON.collect().mkString(", ")
# MAGIC val weight = withVoyage2.select($"weight".as("weight")).toJSON.collect().mkString(", ")
# MAGIC 
# MAGIC val ne = s"{lng: ${boundingBox(3)(0)}, lat: ${boundingBox(3)(1)}}"
# MAGIC val sw = s"{lng: ${boundingBox(0)(0)}, lat: ${boundingBox(0)(1)}}"
# MAGIC 
# MAGIC 
# MAGIC displayHTML(s"""<!DOCTYPE html>
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <meta name="viewport" content="initial-scale=1.0">
# MAGIC     <meta charset="utf-8">
# MAGIC     <style>
# MAGIC        body {
# MAGIC          margin: 4px;
# MAGIC        }
# MAGIC        #map {
# MAGIC         width: 1100px;
# MAGIC         height: 500px;
# MAGIC       }
# MAGIC     </style>
# MAGIC   </head>
# MAGIC   <body>
# MAGIC     <div id="map"></div>
# MAGIC     <script>
# MAGIC       function initMap() {
# MAGIC         var map = new google.maps.Map(document.getElementById('map'), {
# MAGIC           zoom: 8
# MAGIC         });
# MAGIC         
# MAGIC         map.fitBounds(new google.maps.LatLngBounds($sw, $ne))
# MAGIC         
# MAGIC         var infowindow = new google.maps.InfoWindow();
# MAGIC         
# MAGIC         map.addListener('click', function() {
# MAGIC           infowindow.close()
# MAGIC         });
# MAGIC         
# MAGIC         map.data.setStyle(function(feature) {
# MAGIC           var color = 'gray';
# MAGIC           return ({
# MAGIC             icon: null,
# MAGIC             fillColor: color,
# MAGIC             strokeColor: color,
# MAGIC             strokeWeight: 2
# MAGIC           });
# MAGIC         });        
# MAGIC         
# MAGIC         map.data.addListener('click', function(event) {
# MAGIC           infowindow.close();
# MAGIC           var myHTML = 'foo';
# MAGIC           infowindow.setContent("<div style='width:150px; text-align: center;'>"+myHTML+"</div>");
# MAGIC           infowindow.setPosition(event.feature.getGeometry().get());
# MAGIC           infowindow.setOptions({pixelOffset: new google.maps.Size(0,-30)});
# MAGIC           infowindow.open(map);          
# MAGIC         }); 
# MAGIC         
# MAGIC         var heatmap = new google.maps.visualization.HeatmapLayer({
# MAGIC           data: [$points].map(function(i) { return {location: new google.maps.LatLng(i),weight: 1
# MAGIC           }; })
# MAGIC         });
# MAGIC         
# MAGIC         heatmap.setMap(map);
# MAGIC         heatmap.set('opacity', 1.0);
# MAGIC       }
# MAGIC     </script>
# MAGIC     <script async defer
# MAGIC     src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBziwAG-zzHVG8a0-Q6fUA5gopVBQemJxo&callback=initMap&libraries=visualization">
# MAGIC     </script>
# MAGIC   </body>
# MAGIC </html>""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: Visualization
# MAGIC * Visualizing and find outliers

# COMMAND ----------

# DBTITLE 1,Demand Distribution - All Products
# MAGIC %sql select a.locationid,a.SKU,b.name,a.sale
# MAGIC   from (
# MAGIC       select locationid,SKU,count(*) as sale 
# MAGIC         from sales 
# MAGIC         group by locationid,SKU 
# MAGIC         order by locationid,SKU asc) a 
# MAGIC   ,menu b 
# MAGIC   where a.SKU = b.SKU 

# COMMAND ----------

# DBTITLE 1,Demand Distribution - Entrees
# MAGIC %sql select a.SKU,b.name,a.sale 
# MAGIC        from (
# MAGIC           select SKU,count(*) as sale 
# MAGIC                from sales 
# MAGIC                where sku >= 90000 and sku <= 90021 
# MAGIC                group by SKU order by SKU asc) a 
# MAGIC      ,menu b where a.SKU = b.SKU 
# MAGIC      order by a.sale desc

# COMMAND ----------

# DBTITLE 1,Demand Distribution - Drinks
# MAGIC %sql select a.SKU,b.name,a.sale 
# MAGIC        from (
# MAGIC           select SKU,count(*) as sale 
# MAGIC                from sales 
# MAGIC                where sku between 90022 and 90025 
# MAGIC                group by SKU order by SKU asc) a 
# MAGIC      ,menu b where a.SKU = b.SKU 
# MAGIC      order by a.sale desc

# COMMAND ----------

# DBTITLE 1,Demand Distribution - Sides
# MAGIC %sql select a.SKU,b.name,a.sale 
# MAGIC        from (
# MAGIC           select SKU,count(*) as sale 
# MAGIC                from sales 
# MAGIC                where sku between 90026 and 90030 
# MAGIC                group by SKU order by SKU asc) a 
# MAGIC      ,menu b where a.SKU = b.SKU 
# MAGIC      order by a.sale desc

# COMMAND ----------

# DBTITLE 1,Create a Widget for Customizable Insights
# MAGIC %sql 
# MAGIC CREATE WIDGET DROPDOWN Product DEFAULT "Waffle Potato Fries" CHOICES select distinct name from menu

# COMMAND ----------

# DBTITLE 1,Demand for Product Over Time
# MAGIC %sql select a.DATE,a.SKU,b.name,a.sale 
# MAGIC   from (
# MAGIC     select DATE,SKU,count(*) as sale 
# MAGIC       from sales 
# MAGIC       where menu_name = getArgument("Product") 
# MAGIC       group by DATE,SKU 
# MAGIC       order by DATE,SKU asc) a
# MAGIC  ,menu b where a.SKU = b.SKU 

# COMMAND ----------

# DBTITLE 1,Weekly Product Demand 
# MAGIC %sql select a.ts,a.SKU,b.name,a.sale 
# MAGIC   from (
# MAGIC     select ts,SKU,count(*) as sale 
# MAGIC       from sales2 
# MAGIC       where menu_name = getArgument("Product") and DATE in ('2017-01-01', '2017-01-02', '2017-01-03', '2017-01-04', '2017-01-05','2017-01-06', '2017-01-07') 
# MAGIC       group by ts,SKU order by ts,SKU asc) a 
# MAGIC  ,menu b where a.SKU = b.SKU

# COMMAND ----------

# DBTITLE 1,Hourly Demand for Product
# MAGIC %sql select ts, count(*) from sales2 
# MAGIC   where menu_name = getArgument("Product")
# MAGIC   and DATE = '2017-01-02'
# MAGIC   group by ts order by ts asc

# COMMAND ----------

# DBTITLE 1,Most Popular Locations
# MAGIC %sql select location_name, address, city, state, zipcode, count(*) from sales group by location_name, address, city, state, zipcode order by count(*) desc

# COMMAND ----------

# MAGIC %md
# MAGIC clean-up activity after visualization completed

# COMMAND ----------

# MAGIC %sql 
# MAGIC REMOVE WIDGET Product

# COMMAND ----------

# MAGIC %md #### Step 5: Model creation

# COMMAND ----------

from pyspark.ml.classification import *
from pyspark.ml.feature import *
from pyspark.ml import Pipeline
from pyspark.sql.functions import lit

training = sales.sample(True, 0.01).withColumn('label', lit(1))

# COMMAND ----------

# MAGIC %md Define the pipeline using transformers and a classifier

# COMMAND ----------

tokenizer = Tokenizer(inputCol="menu_name", outputCol="menu_terms")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.001)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# COMMAND ----------

# MAGIC %md Train the model using the historical data

# COMMAND ----------

model = pipeline.fit(training)

# COMMAND ----------

# MAGIC %md Make predictions

# COMMAND ----------

prediction = model.transform(training)

# COMMAND ----------

# MAGIC %md Persist the trained model to disk for (real-time) scoring

# COMMAND ----------

model.write().overwrite().save("/mnt/wesley/model/demandforecast/lr")

# COMMAND ----------

display(prediction.select("label", "menu_terms", "features", "prediction"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC The plot above shows that demand index for each ot the items on the menu. 
# MAGIC 
# MAGIC ![Demand Forecast](https://s.yimg.com/ge/labs/v1/files/dsdf.jpg)
# MAGIC 
# MAGIC 1. For the Entrees we can both in the batch as well as streaming data that the most popular item is Spicy chicken sandwich
# MAGIC 2. For the Sides we can both in the batch as well as streaming data that the most popular item is Waffle potato fries. This also happens to be the Most poular item on the menu.
# MAGIC 3. For the Drinks we can both in the batch as well as streaming data that the most popular item is Soft drinks. This also happens to be the Second Most poular item on the menu.