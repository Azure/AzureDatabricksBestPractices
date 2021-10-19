# Databricks notebook source
# MAGIC 
# MAGIC %python
# MAGIC testResults = dict()
# MAGIC 
# MAGIC def toHash(value):
# MAGIC   from pyspark.sql.functions import hash
# MAGIC   from pyspark.sql.functions import abs
# MAGIC   values = [(value,)]
# MAGIC   return spark.createDataFrame(values, ["value"]).select(abs(hash("value")).cast("int")).first()[0]
# MAGIC 
# MAGIC def clearYourResults(passedOnly = True):
# MAGIC   whats = list(testResults.keys())
# MAGIC   for what in whats:
# MAGIC     passed = testResults[what][0]
# MAGIC     if passed or passedOnly == False : del testResults[what]
# MAGIC 
# MAGIC def validateYourSchema(what, df, expColumnName, expColumnType = None):
# MAGIC   label = "{}:{}".format(expColumnName, expColumnType)
# MAGIC   key = "{} contains {}".format(what, label)
# MAGIC 
# MAGIC   try:
# MAGIC     actualType = df.schema[expColumnName].dataType.typeName()
# MAGIC     
# MAGIC     if expColumnType == None: 
# MAGIC       testResults[key] = (True, "validated")
# MAGIC       print("""{}: validated""".format(key))
# MAGIC     elif actualType == expColumnType:
# MAGIC       testResults[key] = (True, "validated")
# MAGIC       print("""{}: validated""".format(key))
# MAGIC     else:
# MAGIC       answerStr = "{}:{}".format(expColumnName, actualType)
# MAGIC       testResults[key] = (False, answerStr)
# MAGIC       print("""{}: NOT matching ({})""".format(key, answerStr))
# MAGIC   except:
# MAGIC       testResults[what] = (False, "-not found-")
# MAGIC       print("{}: NOT found".format(key))
# MAGIC       
# MAGIC def validateYourAnswer(what, expectedHash, answer):
# MAGIC   # Convert the value to string, remove new lines and carriage returns and then escape quotes
# MAGIC   if (answer == None): answerStr = "null"
# MAGIC   elif (answer is True): answerStr = "true"
# MAGIC   elif (answer is False): answerStr = "false"
# MAGIC   else: answerStr = str(answer)
# MAGIC 
# MAGIC   hashValue = toHash(answerStr)
# MAGIC   
# MAGIC   if (hashValue == expectedHash):
# MAGIC     testResults[what] = (True, answerStr)
# MAGIC     print("""{} was correct, your answer: {}""".format(what, answerStr))
# MAGIC   else:
# MAGIC     testResults[what] = (False, answerStr)
# MAGIC     print("""{} was NOT correct, your answer: {}""".format(what, answerStr))
# MAGIC 
# MAGIC def summarizeYourResults():
# MAGIC   html = """<html><body><div style="font-weight:bold; font-size:larger; border-bottom: 1px solid #f0f0f0">Your Answers</div><table style='margin:0'>"""
# MAGIC 
# MAGIC   whats = list(testResults.keys())
# MAGIC   whats.sort()
# MAGIC   for what in whats:
# MAGIC     passed = testResults[what][0]
# MAGIC     answer = testResults[what][1]
# MAGIC     color = "green" if (passed) else "red" 
# MAGIC     passFail = "passed" if (passed) else "FAILED" 
# MAGIC     html += """<tr style='font-size:larger; white-space:pre'>
# MAGIC                   <td>{}:&nbsp;&nbsp;</td>
# MAGIC                   <td style="color:{}; text-align:center; font-weight:bold">{}</td>
# MAGIC                   <td style="white-space:pre; font-family: monospace">&nbsp;&nbsp;{}</td>
# MAGIC                 </tr>""".format(what, color, passFail, answer)
# MAGIC   html += "</table></body></html>"
# MAGIC   displayHTML(html)
# MAGIC 
# MAGIC def logYourTest(path, name, value):
# MAGIC   value = float(value)
# MAGIC   if "\"" in path: raise ValueError("The name cannot contain quotes.")
# MAGIC   
# MAGIC   dbutils.fs.mkdirs(path)
# MAGIC 
# MAGIC   csv = """ "{}","{}" """.format(name, value).strip()
# MAGIC   file = "{}/{}.csv".format(path, name).replace(" ", "-").lower()
# MAGIC   dbutils.fs.put(file, csv, True)
# MAGIC 
# MAGIC def loadYourTestResults(path):
# MAGIC   from pyspark.sql.functions import col
# MAGIC   return spark.read.schema("name string, value double").csv(path)
# MAGIC 
# MAGIC def loadYourTestMap(path):
# MAGIC   rows = loadYourTestResults(path).collect()
# MAGIC   
# MAGIC   map = dict()
# MAGIC   for row in rows:
# MAGIC     map[row["name"]] = row["value"]
# MAGIC   
# MAGIC   return map
# MAGIC 
# MAGIC None

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC 
# MAGIC val testResults = scala.collection.mutable.Map[String, (Boolean, String)]()
# MAGIC 
# MAGIC def toHash(value:String):Int = {
# MAGIC   import org.apache.spark.sql.functions.hash
# MAGIC   import org.apache.spark.sql.functions.abs
# MAGIC   spark.createDataset(List(value)).select(abs(hash($"value")).cast("int")).as[Int].first()
# MAGIC }
# MAGIC 
# MAGIC def clearYourResults(passedOnly:Boolean = true):Unit = {
# MAGIC   val whats = testResults.keySet.toSeq.sorted
# MAGIC   for (what <- whats) {
# MAGIC     val passed = testResults(what)._1
# MAGIC     if (passed || passedOnly == false) testResults.remove(what)
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def validateYourSchema(what:String, df:DataFrame, expColumnName:String, expColumnType:String = null):Unit = {
# MAGIC   val label = s"$expColumnName:$expColumnType"
# MAGIC   val key = s"$what contains $label"
# MAGIC   
# MAGIC   try{
# MAGIC     val actualTypeTemp = df.schema(expColumnName).dataType.typeName
# MAGIC     val actualType = if (actualTypeTemp.startsWith("decimal")) "decimal" else actualTypeTemp
# MAGIC     
# MAGIC     if (expColumnType == null) {
# MAGIC       testResults.put(key,(true, "validated"))
# MAGIC       println(s"""$key: validated""")
# MAGIC       
# MAGIC     } else if (actualType == expColumnType) {
# MAGIC       val answerStr = "%s:%s".format(expColumnName, actualType)
# MAGIC       testResults.put(key,(true, "validated"))
# MAGIC       println(s"""$key: validated""")
# MAGIC       
# MAGIC     } else {
# MAGIC       val answerStr = "%s:%s".format(expColumnName, actualType)
# MAGIC       testResults.put(key,(false, answerStr))
# MAGIC       println(s"""$key: NOT matching ($answerStr)""")
# MAGIC     }
# MAGIC   } catch {
# MAGIC     case e:java.lang.IllegalArgumentException => {
# MAGIC       testResults.put(key,(false, "-not found-"))
# MAGIC       println(s"$key: NOT found")
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def validateYourAnswer(what:String, expectedHash:Int, answer:Any):Unit = {
# MAGIC   // Convert the value to string, remove new lines and carriage returns and then escape quotes
# MAGIC   val answerStr = if (answer == null) "null" 
# MAGIC   else answer.toString
# MAGIC 
# MAGIC   val hashValue = toHash(answerStr)
# MAGIC 
# MAGIC   if (hashValue == expectedHash) {
# MAGIC     testResults.put(what,(true, answerStr))
# MAGIC     println(s"""$what was correct, your answer: ${answerStr}""")
# MAGIC   } else{
# MAGIC     testResults.put(what,(false, answerStr))
# MAGIC     println(s"""$what was NOT correct, your answer: ${answerStr}""")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def summarizeYourResults():Unit = {
# MAGIC   var html = """<html><body><div style="font-weight:bold; font-size:larger; border-bottom: 1px solid #f0f0f0">Your Answers</div><table style='margin:0'>"""
# MAGIC 
# MAGIC   val whats = testResults.keySet.toSeq.sorted
# MAGIC   for (what <- whats) {
# MAGIC     val passed = testResults(what)._1
# MAGIC     val answer = testResults(what)._2
# MAGIC     val color = if (passed) "green" else "red" 
# MAGIC     val passFail = if (passed) "passed" else "FAILED" 
# MAGIC     html += s"""<tr style='font-size:larger; white-space:pre'>
# MAGIC                   <td>${what}:&nbsp;&nbsp;</td>
# MAGIC                   <td style="color:${color}; text-align:center; font-weight:bold">${passFail}</td>
# MAGIC                   <td style="white-space:pre; font-family: monospace">&nbsp;&nbsp;${answer}</td>
# MAGIC                 </tr>"""
# MAGIC   }
# MAGIC   html += "</table></body></html>"
# MAGIC   displayHTML(html)
# MAGIC }
# MAGIC 
# MAGIC def logYourTest(path:String, name:String, value:Double):Unit = {
# MAGIC   if (path.contains("\"")) throw new IllegalArgumentException("The name cannot contain quotes.")
# MAGIC   
# MAGIC   dbutils.fs.mkdirs(path)
# MAGIC 
# MAGIC   val csv = """ "%s","%s" """.format(name, value).trim()
# MAGIC   val file = "%s/%s.csv".format(path, name).replace(" ", "-").toLowerCase
# MAGIC   dbutils.fs.put(file, csv, true)
# MAGIC }
# MAGIC 
# MAGIC def loadYourTestResults(path:String):org.apache.spark.sql.DataFrame = {
# MAGIC   return spark.read.schema("name string, value double").csv(path)
# MAGIC }
# MAGIC 
# MAGIC def loadYourTestMap(path:String):scala.collection.mutable.Map[String,Double] = {
# MAGIC   case class TestResult(name:String, value:Double)
# MAGIC   val rows = loadYourTestResults(path).collect()
# MAGIC   
# MAGIC   val map = scala.collection.mutable.Map[String,Double]()
# MAGIC   for (row <- rows) map.put(row.getString(0), row.getDouble(1))
# MAGIC   
# MAGIC   return map
# MAGIC }
# MAGIC 
# MAGIC displayHTML("""
# MAGIC   <div>Initializing lab environment:</div>
# MAGIC   <li>Declared <b style="color:green">clearYourResults(<i>passedOnly:Boolean=true</i>)</b></li>
# MAGIC   <li>Declared <b style="color:green">validateYourSchema(<i>what:String, df:DataFrame, expColumnName:String, expColumnType:String</i>)</b></li>
# MAGIC   <li>Declared <b style="color:green">validateYourAnswer(<i>what:String, expectedHash:Int, answer:Any</i>)</b></li>
# MAGIC   <li>Declared <b style="color:green">summarizeYourResults()</b></li>
# MAGIC   <li>Declared <b style="color:green">logYourTest(<i>path:String, name:String, value:Double</i>)</b></li>
# MAGIC   <li>Declared <b style="color:green">loadYourTestResults(<i>path:String</i>)</b> returns <b style="color:green">DataFrame</b></li>
# MAGIC   <li>Declared <b style="color:green">loadYourTestMap(<i>path:String</i>)</b> returns <b style="color:green">Map[String,Double]</b></li>
# MAGIC """)