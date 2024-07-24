// Databricks notebook source
// MAGIC %md 
// MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/smolder-solacc.git. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/hl7v2.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Cluster Set Up
// MAGIC 
// MAGIC * Make sure [the Smolder jar file](https://amir-hls.s3.us-east-2.amazonaws.com/public/263572c0_25a1_46ce_9009_2ae456966ea9-smolder_2_12_0_0_1_SNAPSHOT-615ef.jar) is attached to your cluster: If you run the `RUNME` file in this folder, the cluster setup is automated and a Workflow and a `smolder_cluster` is created for you. Feel free to try running this notebook using the Workflow. Alternatively, install [the Smolder jar file](https://amir-hls.s3.us-east-2.amazonaws.com/public/263572c0_25a1_46ce_9009_2ae456966ea9-smolder_2_12_0_0_1_SNAPSHOT-615ef.jar) to the `smolder_cluster` cluster and attach this notebook to run interactively.
// MAGIC * If you run this notebook with the "Run All" option, the last block terminates the streams for you. If you opt to run this notebook block by block, *make sure to cancel your streaming commands, otherwise your cluster will not autoterminate*. 

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP DATABASE IF EXISTS smolder_db CASCADE;

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load HL7 messages to a DataFrame _with streaming_
// MAGIC Now let's read hl7 messages as a spark stream:

// COMMAND ----------

val schema = spark.read.format("hl7").load("/databricks-datasets/rwe/ehr/hl7").schema

val message_stream = spark.readStream.format("hl7") 
  .schema(schema)
  .option("maxFilesPerTrigger", "100")
  .load("/databricks-datasets/rwe/ehr/hl7") 

message_stream.printSchema()

// COMMAND ----------

val messages_df = spark.read.format("hl7").load("/databricks-datasets/rwe/ehr/hl7")
display(
  messages_df.select(explode(col("segments")).alias("segments"))
  .where(col("segments.id").like("PID"))
)

// COMMAND ----------

// MAGIC %md
// MAGIC you can also read a single message

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Define HL7 helper functions
// MAGIC 
// MAGIC HL7 uses an interesting set of delimiters to split records. These are helper functions for working with HL7 messages. These should eventually move inside of Smolder.

// COMMAND ----------

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

def segmentField(segment: String, field: Int): Column = {
  expr("filter(segments, s -> s.id == '%s')[0].fields".format(segment)).getItem(field)
}

def subfield(col: Column, subfield: Int): Column = {
  split(col, "\\^").getItem(subfield)
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Find high utilizers
// MAGIC 
// MAGIC Now, we'll extract the patient identifiers and hospitals that people are visiting, to get a visualization showing high utilizers.
// MAGIC 
// MAGIC Note, there are a lot of small files, so reading the HL7 messages from cloud storage is slow due to the high cost of many small reads.

// COMMAND ----------

val adtEvents = message_stream.select(subfield(segmentField("PID", 4), 0).as("lastName"),
                                subfield(segmentField("PID", 4), 1).as("firstName"),
                                segmentField("PID", 2).as("patientID"),
                                segmentField("EVN", 0).as("eventType"),
                                subfield(segmentField("PV1", 2), 3).as("hospital"))

adtEvents.createOrReplaceTempView("adt_events")

// COMMAND ----------

// DBTITLE 1,See High Utilizers (By Hospital)
// MAGIC %sql
// MAGIC SELECT 
// MAGIC COUNT(eventType) as event_count
// MAGIC , eventType
// MAGIC , patientID
// MAGIC , firstName
// MAGIC , hospital from adt_events 
// MAGIC GROUP BY hospital, eventType, patientID, firstName, lastName
// MAGIC ORDER BY event_count DESC
// MAGIC Limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC COUNT(eventType) as event_count
// MAGIC , patientID
// MAGIC , firstName
// MAGIC  from adt_events 
// MAGIC GROUP BY patientID, firstName
// MAGIC ORDER BY event_count DESC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Persist Our Stream to Delta
// MAGIC Now you can write HL7 stream data to delta bronze layer:

// COMMAND ----------

val output_path = "dbfs:/tmp/HL7_demo/bronze_delta"
val checkpoint_path = "dbfs:/tmp/HL7_demo/checkpoint/" 
val bad_records_path = "dbfs:/tmp/HL7_demo/bronze_delta/badRecordsPath/"

dbutils.fs.mkdirs(output_path)
dbutils.fs.rm(output_path,true) 
dbutils.fs.mkdirs(checkpoint_path)
dbutils.fs.rm(checkpoint_path,true) 
dbutils.fs.mkdirs(bad_records_path)
dbutils.fs.rm(bad_records_path,true) 

// COMMAND ----------

// MAGIC %md
// MAGIC and persist our stream to delta

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val query = adtEvents.writeStream.outputMode("append").format("delta").option("mergeSchema", "true").option("checkpointLocation", checkpoint_path).option("path", output_path).trigger(Trigger.ProcessingTime("5 seconds")).start()
Thread.sleep(120000)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS smolder_db LOCATION 'dbfs:/tmp/HL7_demo/smolder_db'

// COMMAND ----------

// MAGIC %sql
// MAGIC USE smolder_db

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS hl7_adt_stream

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE hl7_adt_stream
// MAGIC USING delta
// MAGIC LOCATION 'dbfs:/tmp/HL7_demo/bronze_delta'

// COMMAND ----------

// MAGIC %sql
// MAGIC DESC HISTORY hl7_adt_stream

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM hl7_adt_stream 

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM hl7_adt_stream@v2

// COMMAND ----------

// MAGIC %md
// MAGIC ## Recap
// MAGIC ##### What we saw:
// MAGIC * Streaming HL7 ingestion & processing
// MAGIC * Near real-time query and analytics of streaming data
// MAGIC * Easily stream into a persisted Delta table
// MAGIC * Leverage Delta functionality for auditability and reproducibility 
// MAGIC 
// MAGIC Now let's gracefully terminate the streaming queries after a few minutes.

// COMMAND ----------

Thread.sleep(120000)
for (s <- spark.streams.active) {
  s.stop
}

// COMMAND ----------

// MAGIC %md
// MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
// MAGIC 
// MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
// MAGIC | :-: | :-:| :-: | :-:|
// MAGIC | Spark|Apache-2.0 License | https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark  |
// MAGIC |Smolder |Apache-2.0 License| https://github.com/databrickslabs/smolder | https://github.com/databrickslabs/smolder/blob/master/LICENSE|

// COMMAND ----------

// MAGIC %md
// MAGIC ## Disclaimers
// MAGIC Databricks Inc. ("Databricks") does not dispense medical, diagnosis, or treatment advice. This Solution Accelerator ("tool") is for informational purposes only and may not be used as a substitute for professional medical advice, treatment, or diagnosis. This tool may not be used within Databricks to process Protected Health Information ("PHI") as defined in the Health Insurance Portability and Accountability Act of 1996, unless you have executed with Databricks a contract that allows for processing PHI, an accompanying Business Associate Agreement (BAA), and are running this notebook within a HIPAA Account. Please note that if you run this notebook within Azure Databricks, your contract with Microsoft applies.
// MAGIC 
// MAGIC All names have been synthetically generated, and do not map back to any actual persons or locations
