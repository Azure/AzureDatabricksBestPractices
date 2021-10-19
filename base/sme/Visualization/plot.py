# Databricks notebook source
dbutils.library.installPyPI("gmaps")
dbutils.library.installPyPI("gmplot")
dbutils.library.restartPython()

# COMMAND ----------

import gmaps
import gmaps.datasets

gmaps.configure(api_key='your api key')

# COMMAND ----------

from gmplot import gmplot

# Place map
gmap = gmplot.GoogleMapPlotter(37.766956, -122.438481, 13)

# Polygon
golden_gate_park_lats, golden_gate_park_lons = zip(*[
    (37.771269, -122.511015),
    (37.773495, -122.464830),
    (37.774797, -122.454538),
    (37.771988, -122.454018),
    (37.773646, -122.440979),
    (37.772742, -122.440797),
    (37.771096, -122.453889),
    (37.768669, -122.453518),
    (37.766227, -122.460213),
    (37.764028, -122.510347),
    (37.771269, -122.511015)
    ])
gmap.plot(golden_gate_park_lats, golden_gate_park_lons, 'cornflowerblue', edge_width=10)

# Scatter points
top_attraction_lats, top_attraction_lons = zip(*[
    (37.769901, -122.498331),
    (37.768645, -122.475328),
    (37.771478, -122.468677),
    (37.769867, -122.466102),
    (37.767187, -122.467496),
    (37.770104, -122.470436)
    ])
gmap.scatter(top_attraction_lats, top_attraction_lons, '#3B0B39', size=40, marker=False)

# Marker
hidden_gem_lat, hidden_gem_lon = 37.770776, -122.461689
gmap.marker(hidden_gem_lat, hidden_gem_lon, 'cornflowerblue')

# Draw
gmap.draw("/dbfs/tmp/bk/my_map.html")

# COMMAND ----------

# we need to change the default size of the div tag so that it shows up on databricks notebook, here I am setting it upto 500x500
with open("/dbfs/tmp/bk/my_map.html", "rt") as fin:
    with open("/dbfs/tmp/bk/my_map_out.html", "wt") as fout:
        for line in fin:
            fout.write(line.replace('100%', '500'))

# COMMAND ----------

# read html file generated into a string so that displatHTML() can render it
fname = "/dbfs/tmp/bk/my_map_out.html"
MapFile = open(fname, 'r', encoding='utf-8')
map1 = MapFile.read()

# COMMAND ----------

displayHTML(map1)

# COMMAND ----------

