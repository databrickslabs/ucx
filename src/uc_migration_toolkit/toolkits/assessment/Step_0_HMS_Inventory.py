# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook helps query the HMS to collect an inventory of what is there to help scope out the UC migration effort. It uses an external catalog API to fetch these meta data so that it doesn't do a auth against each of the storage accounts. 

# COMMAND ----------

# MAGIC %fs rm -r /tmp/uc_assessment/hms

# COMMAND ----------

# MAGIC %fs mkdirs /tmp/uc_assessment/hms

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
# MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
# MAGIC import java.io.{FileWriter, BufferedWriter, File}
# MAGIC
# MAGIC val bw = new BufferedWriter(new FileWriter(new File("/tmp/uc_assessment/hms/metastore_schema.csv"), true));
# MAGIC
# MAGIC bw.write("db,table,format,type,table_location,created_version,created_time,last_access,lib,inputformat,outputformat\n")
# MAGIC val dbs = spark.sharedState.externalCatalog.listDatabases()
# MAGIC for( db <- dbs) {
# MAGIC   //println(s"database: ${db}")
# MAGIC   val tables = spark.sharedState.externalCatalog.listTables(db)
# MAGIC   for (t <- tables) {    
# MAGIC     try {
# MAGIC       //println(s"table: ${t}")
# MAGIC       val table: CatalogTable = spark.sharedState.externalCatalog.getTable(db = db, table = t)
# MAGIC       val row = s"${db},${t},${table.provider.getOrElse("Unknown")},${table.tableType.name},${table.storage.locationUri.getOrElse("None")},${table.createVersion},${table.createTime},${table.lastAccessTime},${table.storage.serde.getOrElse("Unknown")},${table.storage.inputFormat.getOrElse("Unknown")},${table.storage.outputFormat.getOrElse("Unknown")}\n"
# MAGIC       bw.write(row)
# MAGIC     } catch {
# MAGIC       case e: Exception => bw.write(s"${db},${t},Unknown,Unknown,NONE,,,,,,,\n")
# MAGIC     }
# MAGIC   }
# MAGIC
# MAGIC }
# MAGIC
# MAGIC bw.close
