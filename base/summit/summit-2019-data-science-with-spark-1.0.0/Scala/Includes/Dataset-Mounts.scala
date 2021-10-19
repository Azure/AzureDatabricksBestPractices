// Databricks notebook source
// MAGIC 
// MAGIC %scala
// MAGIC val tags = com.databricks.logging.AttributionContext.current.tags
// MAGIC 
// MAGIC //*******************************************
// MAGIC // GET VERSION OF APACHE SPARK
// MAGIC //*******************************************
// MAGIC 
// MAGIC // Get the version of spark
// MAGIC val Array(sparkMajorVersion, sparkMinorVersion, _) = spark.version.split("""\.""")
// MAGIC 
// MAGIC // Set the major and minor versions
// MAGIC spark.conf.set("com.databricks.training.spark.major-version", sparkMajorVersion)
// MAGIC spark.conf.set("com.databricks.training.spark.minor-version", sparkMinorVersion)
// MAGIC 
// MAGIC //*******************************************
// MAGIC // GET VERSION OF DATABRICKS RUNTIME
// MAGIC //*******************************************
// MAGIC 
// MAGIC // Get the version of the Databricks Runtime
// MAGIC val version = tags.collect({ case (t, v) if t.name == "sparkVersion" => v }).head
// MAGIC val runtimeVersion = if (version != "") {
// MAGIC   spark.conf.set("com.databricks.training.job", "false")
// MAGIC   version
// MAGIC } else {
// MAGIC   spark.conf.set("com.databricks.training.job", "true")
// MAGIC   dbutils.widgets.get("sparkVersion")
// MAGIC }
// MAGIC 
// MAGIC val runtimeVersions = runtimeVersion.split("""-""")
// MAGIC // The GPU and ML runtimes push the number of elements out to 5
// MAGIC // so we need to account for every scenario here. There should
// MAGIC // never be a case in which there is less than two so we can fail
// MAGIC // with an helpful error message for <2 or >5
// MAGIC val (dbrVersion, scalaVersion) = {
// MAGIC   runtimeVersions match {
// MAGIC     case Array(d, _, _, _, s) => (d, s.replace("scala", ""))
// MAGIC     case Array(d, _, _, s)    => (d, s.replace("scala", ""))
// MAGIC     case Array(d, _, s)       => (d, s.replace("scala", ""))
// MAGIC     case Array(d, s)          => (d, s.replace("scala", ""))
// MAGIC     case _ =>
// MAGIC       throw new IllegalArgumentException(s"""Dataset-Mounts: Cannot parse version(s) from "${runtimeVersions.mkString(", ")}".""")
// MAGIC   }
// MAGIC }
// MAGIC val Array(dbrMajorVersion, dbrMinorVersion, _*) = dbrVersion.split("""\.""")
// MAGIC 
// MAGIC // Set the the major and minor versions
// MAGIC spark.conf.set("com.databricks.training.dbr.major-version", dbrMajorVersion)
// MAGIC spark.conf.set("com.databricks.training.dbr.minor-version", dbrMinorVersion)
// MAGIC 
// MAGIC //*******************************************
// MAGIC // GET USERNAME AND USERHOME
// MAGIC //*******************************************
// MAGIC 
// MAGIC // Get the user's name
// MAGIC val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
// MAGIC val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
// MAGIC 
// MAGIC val userhome = s"dbfs:/user/$username"
// MAGIC 
// MAGIC // Set the user's name and home directory
// MAGIC spark.conf.set("com.databricks.training.username", username)
// MAGIC spark.conf.set("com.databricks.training.userhome", userhome)
// MAGIC 
// MAGIC //**********************************
// MAGIC // VARIOUS UTILITY FUNCTIONS
// MAGIC //**********************************
// MAGIC 
// MAGIC def assertSparkVersion(expMajor:Int, expMinor:Int):String = {
// MAGIC   val major = spark.conf.get("com.databricks.training.spark.major-version")
// MAGIC   val minor = spark.conf.get("com.databricks.training.spark.minor-version")
// MAGIC 
// MAGIC   if ((major.toInt < expMajor) || (major.toInt == expMajor && minor.toInt < expMinor)) {
// MAGIC     throw new Exception(s"This notebook must be ran on Spark version $expMajor.$expMinor or better, found Spark $major.$minor")
// MAGIC   }
// MAGIC   return s"$major.$minor"
// MAGIC }
// MAGIC 
// MAGIC def assertDbrVersion(expMajor:Int, expMinor:Int):String = {
// MAGIC   val major = spark.conf.get("com.databricks.training.dbr.major-version")
// MAGIC   val minor = spark.conf.get("com.databricks.training.dbr.minor-version")
// MAGIC 
// MAGIC   if ((major.toInt < expMajor) || (major.toInt == expMajor && minor.toInt < expMinor)) {
// MAGIC     throw new Exception(s"This notebook must be ran on Databricks Runtime (DBR) version $expMajor.$expMinor or better, found $major.$minor.")
// MAGIC   }
// MAGIC   return s"$major.$minor"
// MAGIC }
// MAGIC 
// MAGIC //*******************************************
// MAGIC // CHECK FOR REQUIRED VERIONS OF SPARK & DBR
// MAGIC //*******************************************
// MAGIC 
// MAGIC assertDbrVersion(4, 0)
// MAGIC assertSparkVersion(2, 3)
// MAGIC 
// MAGIC displayHTML("Initialized classroom variables & functions...")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC #**********************************
// MAGIC # VARIOUS UTILITY FUNCTIONS
// MAGIC #**********************************
// MAGIC 
// MAGIC def assertSparkVersion(expMajor, expMinor):
// MAGIC   major = spark.conf.get("com.databricks.training.spark.major-version")
// MAGIC   minor = spark.conf.get("com.databricks.training.spark.minor-version")
// MAGIC 
// MAGIC   if (int(major) < expMajor) or (int(major) == expMajor and int(minor) < expMinor):
// MAGIC     msg = "This notebook must run on Spark version {}.{} or better, found.".format(expMajor, expMinor, major, minor)
// MAGIC     raise Exception(msg)
// MAGIC     
// MAGIC   return major+"."+minor
// MAGIC 
// MAGIC def assertDbrVersion(expMajor, expMinor):
// MAGIC   major = spark.conf.get("com.databricks.training.dbr.major-version")
// MAGIC   minor = spark.conf.get("com.databricks.training.dbr.minor-version")
// MAGIC 
// MAGIC   if (int(major) < expMajor) or (int(major) == expMajor and int(minor) < expMinor):
// MAGIC     msg = "This notebook must run on Databricks Runtime (DBR) version {}.{} or better, found.".format(expMajor, expMinor, major, minor)
// MAGIC     raise Exception(msg)
// MAGIC     
// MAGIC   return major+"."+minor
// MAGIC 
// MAGIC #**********************************
// MAGIC # INIT VARIOUS VARIABLES
// MAGIC #**********************************
// MAGIC 
// MAGIC username = spark.conf.get("com.databricks.training.username")
// MAGIC userhome = spark.conf.get("com.databricks.training.userhome")
// MAGIC 
// MAGIC import sys
// MAGIC pythonVersion = spark.conf.set("com.databricks.training.python-version", sys.version[0:sys.version.index(" ")])
// MAGIC 
// MAGIC None # suppress output

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC //**********************************
// MAGIC // CREATE THE MOUNTS
// MAGIC //**********************************
// MAGIC 
// MAGIC def getAwsRegion():String = {
// MAGIC   try {
// MAGIC     import scala.io.Source
// MAGIC     import scala.util.parsing.json._
// MAGIC 
// MAGIC     val jsonString = Source.fromURL("http://169.254.169.254/latest/dynamic/instance-identity/document").mkString // reports ec2 info
// MAGIC     val map = JSON.parseFull(jsonString).getOrElse(null).asInstanceOf[Map[Any,Any]]
// MAGIC     map.getOrElse("region", null).asInstanceOf[String]
// MAGIC 
// MAGIC   } catch {
// MAGIC     // We will use this later to know if we are Amazon vs Azure
// MAGIC     case _: java.io.FileNotFoundException => null
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC def getAzureRegion():String = {
// MAGIC   import com.databricks.backend.common.util.Project
// MAGIC   import com.databricks.conf.trusted.ProjectConf
// MAGIC   import com.databricks.backend.daemon.driver.DriverConf
// MAGIC 
// MAGIC   new DriverConf(ProjectConf.loadLocalConfig(Project.Driver)).region
// MAGIC }
// MAGIC 
// MAGIC val awsAccessKey = "AKIAJBRYNXGHORDHZB4A"
// MAGIC val awsSecretKey = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF"
// MAGIC val awsAuth = s"${awsAccessKey}:${awsSecretKey}"
// MAGIC 
// MAGIC def getAwsMapping(region:String):(String,Map[String,String]) = {
// MAGIC 
// MAGIC   val MAPPINGS = Map(
// MAGIC     "ap-northeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-1/common", Map[String,String]()),
// MAGIC     "ap-northeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-2/common", Map[String,String]()),
// MAGIC     "ap-south-1"     -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-south-1/common", Map[String,String]()),
// MAGIC     "ap-southeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-1/common", Map[String,String]()),
// MAGIC     "ap-southeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-2/common", Map[String,String]()),
// MAGIC     "ca-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ca-central-1/common", Map[String,String]()),
// MAGIC     "eu-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
// MAGIC     "eu-west-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-1/common", Map[String,String]()),
// MAGIC     "eu-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-2/common", Map[String,String]()),
// MAGIC     "eu-west-3"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-3/common", Map[String,String]()),
// MAGIC     "sa-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-sa-east-1/common", Map[String,String]()),
// MAGIC     "us-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-1/common", Map[String,String]()),
// MAGIC     "us-east-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-2/common", Map[String,String]()),
// MAGIC     "us-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]()),
// MAGIC     "_default"       -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]())
// MAGIC   )
// MAGIC 
// MAGIC   MAPPINGS.getOrElse(region, MAPPINGS("_default"))
// MAGIC }
// MAGIC 
// MAGIC def getAzureMapping(region:String):(String,Map[String,String]) = {
// MAGIC 
// MAGIC   // Databricks only wants the query-string portion of the SAS URL (i.e., the part from the "?" onward, including
// MAGIC   // the "?"). But it's easier to copy-and-paste the full URL from the Azure Portal. So, that's what we do.
// MAGIC   // The logic, below, converts these URLs to just the query-string parts.
// MAGIC 
// MAGIC   val EastAsiaAcct = "dbtraineastasia"
// MAGIC   val EastAsiaSas = "https://dbtraineastasia.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T05:02:54Z&st=2018-04-18T21:02:54Z&spr=https&sig=gfu42Oi3QqKjDUMOBGbayQ9WUsxEQ4EdHpI%2BRBCWPig%3D"
// MAGIC 
// MAGIC   val EastUSAcct = "dbtraineastus"
// MAGIC   val EastUSSas = "https://dbtraineastus.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:29:20Z&st=2018-04-18T22:29:20Z&spr=https&sig=drx0LE2W%2BrUTvblQtVU4SiRlWk1WbLUJI6nDvFWIfHA%3D"
// MAGIC 
// MAGIC   val EastUS2Acct = "dbtraineastus2"
// MAGIC   val EastUS2Sas = "https://dbtraineastus2.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:32:30Z&st=2018-04-18T22:32:30Z&spr=https&sig=BB%2FQzc0XHAH%2FarDQhKcpu49feb7llv3ZjnfViuI9IWo%3D"
// MAGIC 
// MAGIC   val NorthCentralUSAcct = "dbtrainnorthcentralus"
// MAGIC   val NorthCentralUSSas = "https://dbtrainnorthcentralus.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:35:29Z&st=2018-04-18T22:35:29Z&spr=https&sig=htJIax%2B%2FAYQINjERk0z%2B0jR%2BBF8MpPK3BdBFa8%2FLAUU%3D"
// MAGIC 
// MAGIC   val NorthEuropeAcct = "dbtrainnortheurope"
// MAGIC   val NorthEuropeSas = "https://dbtrainnortheurope.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:37:15Z&st=2018-04-18T22:37:15Z&spr=https&sig=upIQ%2FoMa4v2aRB8AAB3gBY%2BvybhLwQGS2%2Bsyq0Z3mZw%3D"
// MAGIC 
// MAGIC   val SouthCentralUSAcct = "dbtrainsouthcentralus"
// MAGIC   val SouthCentralUSSas = "https://dbtrainsouthcentralus.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:38:27Z&st=2018-04-18T22:38:27Z&spr=https&sig=OL2amlrWn4X9ABAoWyvaL%2FVIf83GVrAnRL6gpauxqzA%3D"
// MAGIC 
// MAGIC   val SouthEastAsiaAcct = "dbtrainsoutheastasia"
// MAGIC   val SouthEastAsiaSas = "https://dbtrainsoutheastasia.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:39:59Z&st=2018-04-18T22:39:59Z&spr=https&sig=9LFC3cZXe4qWMGABmu%2BuMEAsSKGB%2BfxO0kZTxDAhvF8%3D"
// MAGIC 
// MAGIC   val WestCentralUSAcct = "dbtrainwestcentralus"
// MAGIC   val WestCentralUSSas = "https://dbtrainwestcentralus.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T06:33:55Z&st=2018-04-18T22:33:55Z&spr=https&sig=5tZWw9V4pYuFu7sjTmEcFujAJlcVg3hBl1jgWcSB3Qg%3D"
// MAGIC 
// MAGIC   val WestEuropeAcct = "dbtrainwesteurope"
// MAGIC   val WestEuropeSas = "https://dbtrainwesteurope.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T13:30:09Z&st=2018-04-19T05:30:09Z&spr=https&sig=VRX%2Fp6pC3jJsrPoR7Lz8kvFAUhJC1%2Fzye%2FYvvgFbD5E%3D"
// MAGIC 
// MAGIC   val WestUSAcct = "dbtrainwestus"
// MAGIC   val WestUSSas = "https://dbtrainwestus.blob.core.windows.net/?sv=2017-07-29&ss=b&srt=sco&sp=rl&se=2023-04-19T13:31:40Z&st=2018-04-19T05:31:40Z&spr=https&sig=GRH1J%2FgUiptQHYXLX5JmlICMCOvqqshvKSN4ygqFc3Q%3D"
// MAGIC 
// MAGIC   val WestUS2Acct = "dbtrainwestus2"
// MAGIC   val WestUS2Sas = "https://dbtrainwestus2.blob.core.windows.net/?sv=2018-03-28&ss=bfqt&srt=sco&sp=rl&se=3000-01-01T05:59:59Z&st=2019-01-18T01:16:38Z&spr=https&sig=vY4rsDZhyQ9eq5NblfqVDiPIOmTEQquRIEHB4MH4BTA%3D"
// MAGIC 
// MAGIC   // For each Azure region we support, associate an appropriate Storage Account and SAS token
// MAGIC   // to use to mount /mnt/training (so that we use the version that's closest to the
// MAGIC   // region containing the Databricks instance.)
// MAGIC   // FUTURE RELEASE: New regions are rolled back for this release.  Test new regions before deployment
// MAGIC   
// MAGIC   var MAPPINGS = Map(
// MAGIC     "eastasia"         -> (EastAsiaAcct, EastAsiaSas),
// MAGIC     "eastus"           -> (EastUSAcct, EastUSSas),
// MAGIC     "eastus2"          -> (EastUS2Acct, EastUS2Sas),
// MAGIC     "northcentralus"   -> (NorthCentralUSAcct, NorthCentralUSSas),
// MAGIC     "northeurope"      -> (NorthEuropeAcct, NorthEuropeSas),
// MAGIC     "southcentralus"   -> (SouthCentralUSAcct, SouthCentralUSSas),
// MAGIC     "southeastasia"    -> (SouthEastAsiaAcct, SouthEastAsiaSas),
// MAGIC     "westcentralus"    -> (WestCentralUSAcct, WestCentralUSSas),
// MAGIC     "westeurope"       -> (WestEuropeAcct, WestEuropeSas),
// MAGIC     "westus"           -> (WestUSAcct, WestUSSas),
// MAGIC     "westus2"          -> (WestUS2Acct, WestUS2Sas),
// MAGIC     "_default"         -> (WestUS2Acct, WestUS2Sas) // Following trainers in West-US-2
// MAGIC   ).map { case (key, (acct, url)) => key -> (acct, url.slice(url.indexOf('?'), url.length)) }
// MAGIC 
// MAGIC   val (account: String, sasKey: String) = MAPPINGS.getOrElse(region, MAPPINGS("_default"))
// MAGIC 
// MAGIC   val blob = "training"
// MAGIC   val source = s"wasbs://$blob@$account.blob.core.windows.net/"
// MAGIC   val configMap = Map(
// MAGIC     s"fs.azure.sas.$blob.$account.blob.core.windows.net" -> sasKey
// MAGIC   )
// MAGIC 
// MAGIC   (source, configMap)
// MAGIC }
// MAGIC 
// MAGIC def mountFailed(msg:String): Unit = {
// MAGIC   println(msg)
// MAGIC }
// MAGIC 
// MAGIC def retryMount(source: String, mountPoint: String): Unit = {
// MAGIC   try { 
// MAGIC     // Mount with IAM roles instead of keys for PVC
// MAGIC     dbutils.fs.mount(source, mountPoint)
// MAGIC   } catch {
// MAGIC     case e: Exception => mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC def mount(source: String, extraConfigs:Map[String,String], mountPoint: String): Unit = {
// MAGIC   try {
// MAGIC     dbutils.fs.mount(source, mountPoint, extraConfigs=extraConfigs)
// MAGIC   } catch {
// MAGIC     case ioe: java.lang.IllegalArgumentException => retryMount(source, mountPoint)
// MAGIC     case e: Exception => mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC def autoMount(fix:Boolean = false, failFast:Boolean = false, mountDir:String = "/mnt/training"): Unit = {
// MAGIC   var awsRegion = getAwsRegion()
// MAGIC 
// MAGIC   val (source, extraConfigs) = if (awsRegion != null)  {
// MAGIC     spark.conf.set("com.databricks.training.region.name", awsRegion)
// MAGIC     getAwsMapping(awsRegion)
// MAGIC 
// MAGIC   } else {
// MAGIC     val azureRegion = getAzureRegion()
// MAGIC     spark.conf.set("com.databricks.training.region.name", azureRegion)
// MAGIC     getAzureMapping(azureRegion)
// MAGIC   }
// MAGIC   
// MAGIC   val resultMsg = mountSource(fix, failFast, mountDir, source, extraConfigs)
// MAGIC   displayHTML(resultMsg)
// MAGIC }
// MAGIC 
// MAGIC def mountSource(fix:Boolean, failFast:Boolean, mountDir:String, source:String, extraConfigs:Map[String,String]): String = {
// MAGIC   val mntSource = source.replace(awsAuth+"@", "")
// MAGIC 
// MAGIC   if (dbutils.fs.mounts().map(_.mountPoint).contains(mountDir)) {
// MAGIC     val mount = dbutils.fs.mounts().filter(_.mountPoint == mountDir).head
// MAGIC     if (mount.source == mntSource) {
// MAGIC       return s"""Datasets are already mounted to <b>$mountDir</b> from <b>$mntSource</b>"""
// MAGIC       
// MAGIC     } else if (failFast) {
// MAGIC       throw new IllegalStateException(s"Expected $mntSource but found ${mount.source}")
// MAGIC       
// MAGIC     } else if (fix) {
// MAGIC       println(s"Unmounting existing datasets ($mountDir from $mntSource)")
// MAGIC       dbutils.fs.unmount(mountDir)
// MAGIC       mountSource(fix, failFast, mountDir, source, extraConfigs)
// MAGIC 
// MAGIC     } else {
// MAGIC       return s"""<b style="color:red">Invalid Mounts!</b></br>
// MAGIC                       <ul>
// MAGIC                       <li>The training datasets you are using are from an unexpected source</li>
// MAGIC                       <li>Expected <b>$mntSource</b> but found <b>${mount.source}</b></li>
// MAGIC                       <li>Failure to address this issue may result in significant performance degradation. To address this issue:</li>
// MAGIC                       <ol>
// MAGIC                         <li>Insert a new cell after this one</li>
// MAGIC                         <li>In that new cell, run the command <code style="color:blue; font-weight:bold">%scala fixMounts()</code></li>
// MAGIC                         <li>Verify that the problem has been resolved.</li>
// MAGIC                       </ol>"""
// MAGIC     }
// MAGIC   } else {
// MAGIC     println(s"""Mounting datasets to $mountDir from $mntSource""")
// MAGIC     mount(source, extraConfigs, mountDir)
// MAGIC     return s"""Mounted datasets to <b>$mountDir</b> from <b>$mntSource<b>"""
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC def fixMounts(): Unit = {
// MAGIC   autoMount(true)
// MAGIC }
// MAGIC 
// MAGIC autoMount(true)