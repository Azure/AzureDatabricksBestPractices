# Databricks notebook source
displayHTML(s"""<!DOCTYPE html>
<html>
  <head>
    <meta name="viewport" content="initial-scale=1.0">
    <meta charset="utf-8">
    <title>KML Layers</title>
    <style>
       #map {
        width: 1100px;
        height: 500px;
      }
    </style>
  </head>
  <body>
    <div id="map"></div>
    <script>

      function initMap() {
        var map = new google.maps.Map(document.getElementById('map'), {
          zoom: 14,
          center: {lat: 37.7749, lng: -122.4194}
        });

        var neighborhoodsLayer = new google.maps.KmlLayer({
          url: 'http://uebercomputing.com/img/sf_neighborhoods.kml',
          map: map
        });
        
        ${markersStr}
      }
    </script>
    <script async defer
    src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBziwAG-zzHVG8a0-Q6fUA5gopVBQemJxo&callback=initMap">
    </script>
  </body>
</html>""")

# COMMAND ----------



# COMMAND ----------

dbutils.library.installPyPI("gmaps")
dbutils.library.restartPython()

# COMMAND ----------

