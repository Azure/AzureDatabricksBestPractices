// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC ## Fake Log Server
// MAGIC 
// MAGIC This notebook generates some fake log data to feed into Event Hubs.
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

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC import json
// MAGIC 
// MAGIC from azure.eventhub import EventHubClient, Sender, EventData, Offset
// MAGIC from sseclient import SSEClient as EventSource

// COMMAND ----------

// MAGIC %python
// MAGIC data = {
// MAGIC   "SupervisorStates" :[
// MAGIC     "RUNNING",
// MAGIC     "WAITING",
// MAGIC     "RESTARTING",
// MAGIC     "SUSPENDED"
// MAGIC   ],
// MAGIC   "ProcessNames": [
// MAGIC     "supervisor",
// MAGIC     "master",
// MAGIC     "worker",
// MAGIC     "monitor"
// MAGIC ],
// MAGIC   "FileExtensions" : [
// MAGIC     "csv",
// MAGIC     "tsv",
// MAGIC     "parquet",
// MAGIC     "xml",
// MAGIC     "zip",
// MAGIC     "tar.gz",
// MAGIC     "xslx",
// MAGIC     "txt",
// MAGIC     "orc"
// MAGIC     ],
// MAGIC   "Places": [
// MAGIC     "China",
// MAGIC     "Canada",
// MAGIC     "The Cayman Islands",
// MAGIC     "Nebraska",
// MAGIC     "Russia",
// MAGIC     "Mars",
// MAGIC     "Alpha Centauri",
// MAGIC     "Antarctica",
// MAGIC     "a small unnamed island",
// MAGIC     "Amsterdam",
// MAGIC     "Brussels",
// MAGIC     "San Francisco",
// MAGIC     "New York",
// MAGIC     "Philadelphia",
// MAGIC     "Sri Lanka"
// MAGIC     ],
// MAGIC   "Signals" : [
// MAGIC     "SIGHUP",
// MAGIC     "SIGINT",
// MAGIC     "SIGQUIT",
// MAGIC     "SIGILL",
// MAGIC     "SIGTRAP",
// MAGIC     "SIGABRT",
// MAGIC     "SIGPOLL",
// MAGIC     "SIGIOT",
// MAGIC     "SIGFPE",
// MAGIC     "SIGBUS",
// MAGIC     "SIGSEGV",
// MAGIC     "SIGTERM",
// MAGIC     "SIGURG",
// MAGIC     "SIGSTOP",
// MAGIC     "SIGKILL"
// MAGIC     ],
// MAGIC   "Protocols" : [
// MAGIC     "BitTorrent",
// MAGIC     "SMTP",
// MAGIC     "SSH",
// MAGIC     "FTP",
// MAGIC     "nmap"
// MAGIC     ],
// MAGIC   "Messages" : [
// MAGIC     "(INFO) Device %{deviceID} is online.",
// MAGIC     "(INFO) Network heartbeat check: All OK.",
// MAGIC     "(INFO) Checked file systems for errors: All filesystems OK.",
// MAGIC     "(INFO) Checkpointing off-heap server memory...",
// MAGIC     "(INFO) Rolling %{formattedRandomInt 1 10} log file(s)...",
// MAGIC     "(INFO) Pinging watchdog timer process: Process is alive.",
// MAGIC     "(INFO) Backed up all server logs.",
// MAGIC     "(INFO) Conor is a hipster.",
// MAGIC     "(INFO) Flushed %{formattedRandomInt 20 100} buffers to disk.",
// MAGIC     "(INFO) Alien monitor (aliend): Detected no space aliens.",
// MAGIC     "(INFO) Your mother called. She says you never call her.",
// MAGIC     "(INFO) Waiting for %{choice ProcessNames} process (PID %{pid}) to die.",
// MAGIC     "(INFO) Supervisor has entered %{choice SupervisorStates} state.",
// MAGIC     "(INFO) Sending SIGTERM to %{randomInt 2 5} excess workers.",
// MAGIC     "(INFO) User \"%{username}\" logged in from %{ipAddress}.",
// MAGIC     "(INFO) User \"%{username}\" logged out.",
// MAGIC     "(INFO) User \"%{username}\" logged out due to inactivity (timeout=30 minutes).",
// MAGIC     "(INFO) Databricks is an awesome company.",
// MAGIC     "(WARN) Network heartbeat check: %{randomInt 1 20} server(s) did not respond.",
// MAGIC     "(WARN) Master took %{formattedRandomInt 501 1000} ms to respond (threshold=500 ms)",
// MAGIC     "(WARN) Retrying server log checkpoint.",
// MAGIC     "(WARN) Watchdog timer crashed. Restarting it.",
// MAGIC     "(WARN) Possible cyber-attack from %{choice Places}.",
// MAGIC     "(WARN) Backup server %{ipAddress} is offline.",
// MAGIC     "(WARN) Unable to ping %{choice ProcessNames} at %{ipAddress}. Retrying.",
// MAGIC     "(WARN) Ignoring SIGTERM. It's not that easy to get rid of me.",
// MAGIC     "(WARN) Dropped %{formattedRandomInt 10 20000} corrupt data packets.",
// MAGIC     "(WARN) Space on disk %{disk} is low: Only %{randomInt 100 900} MB available.",
// MAGIC     "(WARN) CPU temperature exceeds 40º Celsius.",
// MAGIC     "(WARN) There is no such thing as a free lunch.",
// MAGIC     "(WARN) The \"check engine\" light just went on.",
// MAGIC     "(WARN) User \"%{username}\" (IP %{ipAddress}) disconnected without logging off. Session cancelled.",
// MAGIC     "(ERROR) The zoo animals have escaped!",
// MAGIC     "(ERROR) Device %{deviceID} is offline",
// MAGIC     "(ERROR) CChheecckk dduupplleexx sseettttiinngg..",
// MAGIC     "(ERROR) Worker %{randomInt 1 10} died with %{choice Signals}. Restarting it.",
// MAGIC     "(ERROR) Alien monitor (aliend): %{randomInt 2 5} space aliens are fiddling with the hardware!",
// MAGIC     "(ERROR) Network heartbeat check FAILED: Master host down?",
// MAGIC     "(ERROR) Server log backup FAILED. See backup logs.",
// MAGIC     "(ERROR) One of the flayrods has gone out askew on treadle.",
// MAGIC     "(ERROR) \"/var/data/%{textfile}\": No such file or directory (ENOENT)",
// MAGIC     "(ERROR) \"/var/data/%{textfile}\": Permission denied (EACCES)",
// MAGIC     "(ERROR) Directory index of \"/var/data\" is forbidden.",
// MAGIC     "(ERROR) Blocked %{choice Protocols} connection from %{ipAddress}.",
// MAGIC     "(ERROR) Detected incoming DDoS from %{ipAddress}.",
// MAGIC     "(ERROR) Blocked Little Bobby Tables attack (https://xkcd.com/327/) from %{ipAddress}.",
// MAGIC     "(ERROR) CPU temperature exceeds 50º Celsius. Shutting down.",
// MAGIC     "(ERROR) EFOODFAIL (Waiter, there's a fly in my soup).",
// MAGIC     "(ERROR) /dev/lp error: \"PC LOAD LETTER\"",
// MAGIC     "(ERROR) Extra characters at end of line.",
// MAGIC     "(ERROR) User \"%{username}\" failed to log in 3 times. Disabling account.",
// MAGIC     "(ERROR) Connection to %{ipAddress} terminated unexpectedly. Reconnecting.",
// MAGIC     "(ERROR) Unable to authenticate user \"%{username}\": LDAP server isn't responding.",
// MAGIC     "(ERROR) Failed to open \"/mnt/fileserver0%{randomInt 1 3}/%{textfile}\": NFS timeout.",
// MAGIC     "(ERROR) Failed to write checkpoint file \"/var/checkpoint/%{basefile}\": No space left on device (ENOSPC)",
// MAGIC     "(ERROR) Detected clock skew! Restarting NTP server.",
// MAGIC     "(ERROR) Unable to write to \"/var/data/%{textfile}\": Permission denied (EACCES)",
// MAGIC     "(ERROR) Unable to mount %{disk}. (Time to run fsck?)",
// MAGIC     "(ERROR) \"/var/data/%{textfile}\" is corrupt. Deleting it.",
// MAGIC     "(ERROR) Unable to connect to \"gopher://gopher.example.com\": Unknown scheme."
// MAGIC     ]
// MAGIC }

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC import datetime, random, re, string
// MAGIC 
// MAGIC def deviceID():
// MAGIC   return hex(random.randint(1, 65535))
// MAGIC 
// MAGIC def disk():
// MAGIC   return "/dev/sd{}".format(random.randint(1, 5))
// MAGIC 
// MAGIC def ipAddress():
// MAGIC   return "{}.{}.{}.{}".format(
// MAGIC     random.randint(0,255),
// MAGIC     random.randint(0,255),
// MAGIC     random.randint(0,255),
// MAGIC     random.randint(0,255)
// MAGIC   )
// MAGIC 
// MAGIC def name():
// MAGIC   return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
// MAGIC 
// MAGIC def pid():
// MAGIC   return random.randint(200, 65535)
// MAGIC 
// MAGIC def textfile():
// MAGIC   extension = random.choice(data['FileExtensions'])
// MAGIC   return "{}.{}".format(name(), extension)
// MAGIC 
// MAGIC replacement_function = {
// MAGIC   "basefile" : name,
// MAGIC   "choice" : lambda x: random.choice(data[x]),
// MAGIC   "deviceID" : deviceID,
// MAGIC   "disk" : disk,
// MAGIC   "formattedRandomInt" : lambda a, b: "{:,}".format(random.randint(a, b)),
// MAGIC   "ipAddress" : ipAddress,
// MAGIC   "pid" : pid,
// MAGIC   "randomInt" : lambda a, b: random.randint(a, b),
// MAGIC   "textfile" : textfile,
// MAGIC   "username" : name
// MAGIC }
// MAGIC 
// MAGIC def message():
// MAGIC   now = datetime.datetime.now()
// MAGIC   
// MAGIC   match_pattern = re.compile("%{(.+?)}")
// MAGIC   this_message = random.choice(data['Messages'])
// MAGIC   
// MAGIC   matches = re.search("%{(.+?)}", this_message)
// MAGIC   if matches:
// MAGIC     for match in matches.groups():
// MAGIC       this_message = this_message.replace("%{"+match+"}", replacement(match))
// MAGIC   return "[{}] {}".format(now.strftime("%Y/%m/%d %H:%M:%S.%f"), this_message)
// MAGIC 
// MAGIC def replacement(match): 
// MAGIC   key, *args = match.split()
// MAGIC   func = replacement_function[key]
// MAGIC 
// MAGIC   if args == []:
// MAGIC     return str(func())
// MAGIC   
// MAGIC   if key in ['formattedRandomInt', 'randomInt']:
// MAGIC     return str(func(int(args[0]), int(args[1])))
// MAGIC   return str(func(args[0]))

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC connString = pcString.replace(".net/;", ".net/{}/;".format(uniqueEHName))
// MAGIC eh = EventHubClient.from_connection_string(connString)
// MAGIC 
// MAGIC sender = eh.add_sender(partition="0")
// MAGIC 
// MAGIC eh.run()
// MAGIC displayHTML("")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC for i in range(eventCount):
// MAGIC     sender.send(EventData(message()))

// COMMAND ----------

// MAGIC %python
// MAGIC displayHTML("All done!")