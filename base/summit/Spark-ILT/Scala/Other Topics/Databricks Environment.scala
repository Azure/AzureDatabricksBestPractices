// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # The Databricks Environment
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC - Set the stage for learning on the Databricks platform
// MAGIC - Demonstrate how to develop & execute code within a notebook.
// MAGIC - Introduce the Databricks File System (DBFS)
// MAGIC - Introduce `dbutils`
// MAGIC - Review the various "Magic Commands"
// MAGIC - Review various built-in commands that facilitate working with the notebooks

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Scala, Python, R, SQL
// MAGIC 
// MAGIC * Each notebook is tied to a specific language: **Scala**, **Python**, **SQL** or **R**
// MAGIC * Run the cell below using one of the following options:
// MAGIC   * **CTRL+ENTER** or **CMD+RETURN**
// MAGIC   * **SHIFT+ENTER** or **SHIFT+RETURN** to run the cell and move to the next one
// MAGIC   * Using **Run Cell**, **Run All Above** or **Run All Below** as seen here<br/><img style="box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>
// MAGIC 
// MAGIC Feel free to tweak the code below if you like:

// COMMAND ----------

println("I'm running Scala!")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Magic Commands
// MAGIC * Magic Commands are specific to the Databricks notebooks
// MAGIC * They are very similar to Magic Commands found in comparable notebook products
// MAGIC * These are built-in commands that do not apply to the notebook's default language
// MAGIC * A single percent (%) symbol at the start of a cell identifies a Magic Commands

// COMMAND ----------

// MAGIC %md
// MAGIC ### Magic Command: &percnt;sh
// MAGIC For example, **&percnt;sh** allows us to execute shell commands on the driver

// COMMAND ----------

// MAGIC %sh ps | grep 'java'

// COMMAND ----------

// MAGIC %md
// MAGIC ### Magic Command: Other Languages
// MAGIC Additional Magic Commands allow for the execution of code in languages other than the notebook's default:
// MAGIC * **&percnt;python** 
// MAGIC * **&percnt;scala** 
// MAGIC * **&percnt;sql** 
// MAGIC * **&percnt;r** 

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC println("Hello Scala!")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC print("Hello Python!")

// COMMAND ----------

// MAGIC %r
// MAGIC 
// MAGIC print("Hello R!", quote=FALSE)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select "Hello SQL!"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Magic Command: &percnt;md
// MAGIC 
// MAGIC Our favorite Magic Command **&percnt;md** allows us to render Markdown in a cell:
// MAGIC * Double click this cell to begin editing it
// MAGIC * Then hit `Esc` to stop editing
// MAGIC 
// MAGIC # Title One
// MAGIC ## Title Two
// MAGIC ### Title Three
// MAGIC 
// MAGIC This is a test of the emergency broadcast system. This is only a test.
// MAGIC 
// MAGIC This is text with a **bold** word in it.
// MAGIC 
// MAGIC This is text with an *italicized* word in it.
// MAGIC 
// MAGIC This is an ordered list
// MAGIC 0. once
// MAGIC 0. two
// MAGIC 0. three
// MAGIC 
// MAGIC This is an unordered list
// MAGIC * apples
// MAGIC * peaches
// MAGIC * bananas
// MAGIC 
// MAGIC Links/Embedded HTML: <a href="http://bfy.tw/19zq" target="_blank">What is Markdown?</a>
// MAGIC 
// MAGIC Images:  
// MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
// MAGIC 
// MAGIC And of course, tables:
// MAGIC 
// MAGIC | Name  | Age | Sex    |
// MAGIC |-------|-----|--------|
// MAGIC | Tom   | 32  | Male   |
// MAGIC | Mary  | 29  | Female |
// MAGIC | Dick  | 73  | Male   |
// MAGIC | Sally | 55  | Female |

// COMMAND ----------

// MAGIC %md
// MAGIC ### Magic Command: &percnt;run
// MAGIC * You can run a notebook from another notebook by using the Magic Command **%run** 
// MAGIC * All variables & functions defined in that other notebook will become available in your current notebook
// MAGIC 
// MAGIC For example, The following cell should fail to execute because the variable `username` has not yet been declared:

// COMMAND ----------

// Uncomment and try this:
// println("username: " + username)

// COMMAND ----------

// MAGIC %md
// MAGIC But we can declare it and a handful of other variables and functions buy running this cell:

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC In this case, the notebook `Classroom Setup` declares the following:
// MAGIC   * The variable `username`
// MAGIC   * The variable `userhome`
// MAGIC   * The function `assertSparkVersion(..)`
// MAGIC   * And others...

// COMMAND ----------

println("username: " + username)
println("userhome: " + userhome)

// COMMAND ----------

// MAGIC %md
// MAGIC We will use those variables and functions throughout this class.
// MAGIC 
// MAGIC One of the other things `Classroom Setup` does for us is to mount all the datasets needed for this class into the Databricks File System.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Databricks File System - DBFS
// MAGIC * DBFS is a layer over a cloud-based object store
// MAGIC * Files in DBFS are persisted to the object store
// MAGIC * The lifetime of files in the DBFS are **NOT** tied to the lifetime of our cluster

// COMMAND ----------

// MAGIC %md
// MAGIC ### Mounting Data into DBFS
// MAGIC * Mounting other object stores into DBFS gives Databricks users access via the file system
// MAGIC * This is just one of many techniques for pulling data into Spark
// MAGIC * The datasets needed for this class have already been mounted for us with the call to `%run "../Includes/Classroom Setup"`
// MAGIC * We will confirm that in just a few minutes

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC See also <a href="https://docs.azuredatabricks.net/user-guide/dbfs-databricks-file-system.html" target="_blank">Databricks File System - DBFS</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Databricks Utilities - dbutils
// MAGIC * You can access the DBFS through the Databricks Utilities class (and other file IO routines).
// MAGIC * An instance of DBUtils is already declared for us as `dbutils`.
// MAGIC * For in-notebook documentation on DBUtils you can execute the command `dbutils.help()`.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC See also <a href="https://docs.azuredatabricks.net/user-guide/dbutils.html" target="_blank">Databricks Utilities - dbutils</a>

// COMMAND ----------

dbutils.help()

// COMMAND ----------

// MAGIC %md
// MAGIC Additional help is available for each sub-utility:
// MAGIC * `dbutils.fs.help()`
// MAGIC * `dbutils.meta.help()`
// MAGIC * `dbutils.notebook.help()`
// MAGIC * `dbutils.widgets.help()`
// MAGIC 
// MAGIC Let's take a look at the file system utilities, `dbutils.fs`

// COMMAND ----------

dbutils.fs.help()

// COMMAND ----------

// MAGIC %md
// MAGIC ### dbutils.fs.mounts()
// MAGIC * As previously mentioned, all our datasets should already be mounted
// MAGIC * We can use `dbutils.fs.mounts()` to verify that assertion
// MAGIC * This method returns a collection of `MountInfo` objects, one for each mount

// COMMAND ----------

val mounts = dbutils.fs.mounts()

for (mount <- mounts) {
  println(mount.mountPoint + " >> " + mount.source)
}

println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ### dbutils.fs.ls(..)
// MAGIC * And now we can use `dbutils.fs.ls(..)` to view the contents of that mount
// MAGIC * This method returns a collection of `FileInfo` objects, one for each item in the specified directory

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC See also <a href="https://docs.azuredatabricks.net/api/latest/dbfs.html#dbfsfileinfo" target="_blank">FileInfo</a>

// COMMAND ----------

val files = dbutils.fs.ls("/mnt/training/")

for (fileInfo <- files) {
  println(fileInfo.path)
}

println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ### display(..)
// MAGIC 
// MAGIC Besides printing each item returned from `dbutils.fs.ls(..)` we can also pass that collection to another Databricks specific command called `display(..)`.

// COMMAND ----------

val files = dbutils.fs.ls("/mnt/training/")

display(files)

// COMMAND ----------

// MAGIC %md
// MAGIC The `display(..)` command is overloaded with a lot of other capabilities:
// MAGIC * Presents up to 1000 records.
// MAGIC * Exporting data as CSV.
// MAGIC * Rendering a multitude of different graphs.
// MAGIC * Rendering geo-located data on a world map.
// MAGIC 
// MAGIC And as we will see later, it is also an excellent tool for previewing our data in a notebook.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Magic Command: &percnt;fs
// MAGIC 
// MAGIC There is at least one more trick for looking at the DBFS.
// MAGIC 
// MAGIC It is a wrapper around `dbutils.fs` and it is the Magic Command known as **&percnt;fs**.
// MAGIC 
// MAGIC The following call is equivalent to the previous call, `display( dbutils.fs.ls("/mnt/training") )` - there is no real difference between the two.

// COMMAND ----------

// MAGIC %fs ls /mnt/training

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) displayHTML(..)
// MAGIC 
// MAGIC One more Databricks-specific command we may use later is `displayHTML(..)`
// MAGIC 
// MAGIC This command will render your custom HTML in an `IFRAME` and then present that in our notebook and/or dashboard.
// MAGIC 
// MAGIC The really nice thing about this is that you can make this call directly from your code making it really easy to customize the presentation of your data.

// COMMAND ----------

val choices = Array("red", "green", "blue")

var html = """
<body>
  <h1>This is HTML</h1>
  <div style="color:red">What is your favorite color?</div>
"""

for (choice <- choices) {
  html += s"""<label for="$choice" style="margin:0"><input id="$choice" type="radio" name="answer" style="vertical-align:top"> $choice</label><br/>"""
}

html += """
</body>
"""

displayHTML(html)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Widgets
// MAGIC 
// MAGIC Input widgets allow you to add parameters to your notebooks and dashboards. The widget API consists of calls to create different types of input widgets, remove them, and get bound values.
// MAGIC 
// MAGIC Widgets are best for:
// MAGIC 
// MAGIC  * Building a notebook or dashboard that is re-executed with different parameters
// MAGIC  * Quickly exploring results of a single query with different parameters
// MAGIC 
// MAGIC 
// MAGIC View the documentation for the widget API in Scala, Python, and R with the following command:

// COMMAND ----------

dbutils.widgets.help()

// COMMAND ----------

// MAGIC %md
// MAGIC Execute the following cell to create two widgets on the top of the screen.

// COMMAND ----------

dbutils.widgets.combobox("hihey","Hi",Array("Hi", "Hey", "Hello"), "Oh,")
dbutils.widgets.text("name", "Anonymous", "Your name")

// COMMAND ----------

// MAGIC %md
// MAGIC Change the values of the widgets and watch the result of the next cell change:

// COMMAND ----------

displayHTML("<h2>" + dbutils.widgets.get("hihey") + " " + dbutils.widgets.get("name") + ", Welcome to Databricks!</h2>")

// COMMAND ----------

// MAGIC %md
// MAGIC Now remove all the widgets:

// COMMAND ----------

dbutils.widgets.removeAll()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Learning More
// MAGIC 
// MAGIC We like to encourage you to explore the documentation to learn more about the various features of the Databricks platform and notebooks.
// MAGIC * <a href="https://docs.azuredatabricks.net/user-guide/index.html" target="_blank">User Guide</a>
// MAGIC * <a href="https://docs.azuredatabricks.net/user-guide/notebooks/index.html" target="_blank">User Guide / Notebooks</a>
// MAGIC * <a href="https://docs.azuredatabricks.net/administration-guide/index.html" target="_blank">Administration Guide</a>
// MAGIC * <a href="https://docs.azuredatabricks.net/api/index.html" target="_blank">REST API</a>
// MAGIC * <a href="https://docs.azuredatabricks.net/release-notes/index.html" target="_blank">Release Notes</a>
// MAGIC * <a href="https://docs.azuredatabricks.net" target="_blank">And much more!</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>