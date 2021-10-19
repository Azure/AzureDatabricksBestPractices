// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Wikipedia Edits Stream Server
// MAGIC 
// MAGIC This notebook grabs data from a Wikipedia web server into Event Hubs.
// MAGIC 
// MAGIC ### Library Requirements
// MAGIC 
// MAGIC 1. the Maven library with coordinate `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.7`
// MAGIC    - this allows Databricks `spark` session to communicate with an Event Hub
// MAGIC 2. the Python library `azure-eventhub`
// MAGIC    - this is allows the Python kernel to stream content to an Event Hub
// MAGIC 3. the Python library `sseclient`
// MAGIC    - this is used to create a streaming client to an existing streaming server
// MAGIC 
// MAGIC Documentation on how to install Python libraries:
// MAGIC https://docs.azuredatabricks.net/user-guide/libraries.html#pypi-libraries
// MAGIC 
// MAGIC Documentation on how to install Maven libraries:
// MAGIC https://docs.azuredatabricks.net/user-guide/libraries.html#maven-or-spark-package
// MAGIC 
// MAGIC You can use <b>Run All</b> in the top menu bar next to <b>Permissions</b> to run this notebook.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC dbutils.widgets.text("CONNECTION_STRING", "", "Connection String")
// MAGIC dbutils.widgets.text("EVENT_HUB_NAME", "fake-log-server", "Event Hub")
// MAGIC dbutils.widgets.text("EVENT_COUNT","1000","Event Count")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC pcString = dbutils.widgets.get("CONNECTION_STRING")
// MAGIC uniqueEHName = dbutils.widgets.get("EVENT_HUB_NAME")
// MAGIC 
// MAGIC # set this to number of records you want
// MAGIC eventCount = int(dbutils.widgets.get("EVENT_COUNT"))
// MAGIC 
// MAGIC assert pcString != "", ": The Primary Connection String must be non-empty"
// MAGIC assert uniqueEHName != "", ": The Unique Event Hubs Name must be non-empty"
// MAGIC assert (eventCount != "") & (eventCount > 0), ": The number of events must be non-empty and greater than 0"
// MAGIC 
// MAGIC connectionString = pcString.replace(".net/;", ".net/{}/;".format(uniqueEHName))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Create an `EventHubClient` with a sender and run the client.
// MAGIC 
// MAGIC Import Libraries necessary to run the server.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from azure.eventhub import EventHubClient, Sender, EventData, Offset
// MAGIC from sseclient import SSEClient as EventSource
// MAGIC 
// MAGIC eh = EventHubClient.from_connection_string(connectionString)
// MAGIC 
// MAGIC sender = eh.add_sender(partition="0")
// MAGIC 
// MAGIC eh.run()

// COMMAND ----------

// MAGIC %md
// MAGIC Augment with geo data.

// COMMAND ----------

// MAGIC %python
// MAGIC import json, random
// MAGIC 
// MAGIC geo_data = [{"city" : "Sydney", "country" : "Australia", "countrycode3" : "AUS", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Sofia", "country" : "Bulgaria", "countrycode3" : "BGR", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Calgary", "country" : "Canada", "countrycode3" : "CAN", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Shantou", "country" : "China", "countrycode3" : "CHN", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Giza", "country" : "Egypt", "countrycode3" : "EGY", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Munich", "country" : "Germany", "countrycode3" : "DEU", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Chennai", "country" : "India", "countrycode3" : "IND", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Jaipur", "country" : "India", "countrycode3" : "IND", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Nagpur", "country" : "India", "countrycode3" : "IND", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Tehran", "country" : "Iran", "countrycode3" : "IRN", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Hiroshima", "country" : "Japan", "countrycode3" : "JPN", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Kuala Lumpur", "country" : "Malaysia", "countrycode3" : "MYS", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Fez", "country" : "Morocco", "countrycode3" : "MAR", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Maputo", "country" : "Mozambique", "countrycode3" : "MOZ", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Mandalay", "country" : "Myanmar", "countrycode3" : "MMR", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Gujranwala", "country" : "Pakistan", "countrycode3" : "PAK", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Manila", "country" : "Philippines", "countrycode3" : "PHL", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Riyadh", "country" : "Saudi Arabia", "countrycode3" : "SAU", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Dakar", "country" : "Senegal", "countrycode3" : "SEN", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Dubai", "country" : "United Arab Emirates", "countrycode3" : "ARE", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Fresno", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "93650"},
// MAGIC {"city" : "Cincinnati", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Ohio", "PostalCode" : "41073"},
// MAGIC {"city" : "San Diego", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "91945"},
// MAGIC {"city" : "Portland", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Oregon", "PostalCode" : "97035"},
// MAGIC {"city" : "Long Beach", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "90712"},
// MAGIC {"city" : "San Antonio", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Texas", "PostalCode" : "78006"},
// MAGIC {"city" : "Kansas City", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Missouri", "PostalCode" : "64030"},
// MAGIC {"city" : "Los Angeles", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "90001"},
// MAGIC {"city" : "Memphis", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Tennessee", "PostalCode" : "37501"},
// MAGIC {"city" : "Tucson", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Arizona", "PostalCode" : "85641"},
// MAGIC {"city" : "Rochester", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "New York", "PostalCode" : "14602"},
// MAGIC {"city" : "Denver", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Colorado", "PostalCode" : "80014"},
// MAGIC {"city" : "Virginia Beach", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Virginia", "PostalCode" : "23450 "},
// MAGIC {"city" : "Montgomery", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Alabama", "PostalCode" : "36043"},
// MAGIC {"city" : "Plano", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Texas", "PostalCode" : "75023"},
// MAGIC {"city" : "Huntington", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "New York", "PostalCode" : "11721"},
// MAGIC {"city" : "Henderson", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Nevada", "PostalCode" : "89002"},
// MAGIC {"city" : "St. Paul", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Minnesota", "PostalCode" : "55101"},
// MAGIC {"city" : "Birmingham", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Alabama", "PostalCode" : "35005"},
// MAGIC {"city" : "St. Louis", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Missouri", "PostalCode" : "63101"}];
// MAGIC 
// MAGIC def add_geo_data(event):
// MAGIC     event_data = json.loads(event.data)
// MAGIC     event_data["geolocation"] = random.choice(geo_data)
// MAGIC     return json.dumps(event_data)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Stream events from the Wiki Streaming Server. This code breaks the loop after it hits `Event Count`. 
// MAGIC 
// MAGIC To read more events, alter the variable `Event Count` in the widget at the top. 
// MAGIC 
// MAGIC This cell can be run multiple times.

// COMMAND ----------

// MAGIC %python
// MAGIC wikiChangesURL = 'https://stream.wikimedia.org/v2/stream/recentchange'
// MAGIC         
// MAGIC for i, event in enumerate(EventSource(wikiChangesURL)):
// MAGIC     if event.event == 'message' and event.data != '':
// MAGIC         sender.send(EventData(add_geo_data(event)))
// MAGIC     if i > eventCount:
// MAGIC         print("OK")
// MAGIC         break

// COMMAND ----------

// MAGIC %python
// MAGIC displayHTML("All done!")