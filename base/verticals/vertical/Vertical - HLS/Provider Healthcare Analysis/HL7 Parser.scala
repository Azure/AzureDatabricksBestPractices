// Databricks notebook source
// MAGIC %md
// MAGIC ***HAPI (HL7 application programming interface; pronounced "happy") is an open-source, object-oriented HL7 2.x parser for Java. HL7 ( http://hl7.org ) is a messaging specification for healthcare information systems. This project is not affiliated with the HL7 organization; we are just writing some software that conforms to their specification. The project was initiated by University Health Network (a large multi-site teaching hospital in Toronto, Canada).***
// MAGIC 
// MAGIC The Example are taken to generate the HL7 message below is taken from the [**Hapi project**](https://hapifhir.github.io/hapi-hl7v2/) 
// MAGIC 
// MAGIC * We will use use this message to test the model created in the previous notebook

// COMMAND ----------

dbutils.widgets.text("hl7_message", "", "HL7 Message")

// COMMAND ----------

// DBTITLE 1,Import the HL7 Libraries
import ca.uhn.hl7v2.DefaultHapiContext
import ca.uhn.hl7v2.HapiContext
import ca.uhn.hl7v2.model.GenericMessage
import ca.uhn.hl7v2.model.Message
import ca.uhn.hl7v2.parser.CanonicalModelClassFactory
import ca.uhn.hl7v2.parser.DefaultModelClassFactory
import ca.uhn.hl7v2.parser.PipeParser
import ca.uhn.hl7v2.util.Terser

// COMMAND ----------

// DBTITLE 1,HL7 message used to testing the model
val v23message = "MSH|^~\\&|ULTRA|TML|OLIS|OLIS|200905011130|THOMAS HOSPITAL|ORU^R01|20169838-v23|T|2.3|||||T|AL\r"+ "PID|||7005728^^^TML^MR||TEST^RACHEL^DIAMOND||19310313|F|||200 ANYWHERE ST^^TORONTO^ON^M6G 2T9||(416)888-8888||||||1014071185^KR\r"+ "PV1|1||OLIS||||OLIST^BLAKE^DONALD^THOR^^^^^921379^^^^OLIST\r"+ "ORC|RE||T09-100442-RET-0^^OLIS_Site_ID^ISO|||||||||OLIST^BLAKE^DONALD^THOR^^^^L^921379\r"+ "OBR|0||T09-100442-RET-0^^OLIS_Site_ID^ISO|RET^RETICULOCYTE COUNT^HL79901literal|||200905011106|||||||200905011106||OLIST^BLAKE^DONALD^THOR^^^^L^921379||7870279|7870279|T09-100442|MOHLTC|200905011130||B7|F||1^^^200905011106^^R\r" + "OBX|1|ST|||Test Value";

// COMMAND ----------

// MAGIC %md
// MAGIC ### We will now parse the above HL7 Message to extract the columns we are interested in for generating the Dataset

// COMMAND ----------

val context = new DefaultHapiContext();

val mcf = new CanonicalModelClassFactory("2.5");
context.setModelClassFactory(mcf);

// Pass the MCF to the parser in its constructor
val parser = context.getPipeParser();

// COMMAND ----------

// The parser parses the v2.3 message to a "v25" structure
val msg = parser.parse(v23message).asInstanceOf[ca.uhn.hl7v2.model.v25.message.ORU_R01];

// COMMAND ----------

case class PatientRec(Hospital_Name: String, State_Code: String, Measure_Name: String)

val caseClassDS = Seq(PatientRec(msg.getMSH().getSecurity().getValue(), msg.getMSH().getCountryCode().getValue(),"READM-30-COPD-HRRP")).toDS()

display(caseClassDS)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Reloading the earlier model created in previous notebook

// COMMAND ----------

import org.apache.spark.ml.{Pipeline, PipelineModel}
val model = PipelineModel.load("/tmp/wesley/spark-logistic-regression-model")

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

val indexer1 = (new StringIndexer()
                   .setInputCol("Hospital_Name")
                   .setOutputCol("Hospital_Name_Index")
                   .fit(caseClassDS))

val indexed1 = indexer1.transform(caseClassDS)

val indexer2 = (new StringIndexer()
                   .setInputCol("State_Code")
                   .setOutputCol("State_Code_Index")
                   .fit(indexed1))

val indexed2 = indexer2.transform(indexed1)

val indexer3 = (new StringIndexer()
                   .setInputCol("Measure_Name")
                   .setOutputCol("Measure_Name_Index")
                   .fit(indexed2))

val indexed3 = indexer3.transform(indexed2)

// COMMAND ----------

// DBTITLE 1,Applying the model on the test dataset
val sampledPatientRec = model.transform(indexed3)
sampledPatientRec.createOrReplaceTempView("sampledPatientRec")

// COMMAND ----------

display(sampledPatientRec)

// COMMAND ----------

dbutils.widgets.remove("hl7_message")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Results interpretation
// MAGIC The plot above shows Index used to measure each of the patent admission days. 
// MAGIC 
// MAGIC ![Churn-Index](http://www.ngdata.com/wp-content/uploads/2016/05/churn.jpg)
// MAGIC 
// MAGIC we have demonstrated prediction of expected ratio of readmission for a given patient based on linear regression on Databricks Notebook.