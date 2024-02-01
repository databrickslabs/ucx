// Databricks notebook source
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters
import org.apache.hadoop.fs._
import org.yaml.snakeyaml.Yaml
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,lower,upper}

// must follow the same structure as databricks.labs.ucx.hive_metastore.tables.Table
case class TableDetails(catalog: String, database: String, name: String, object_type: String,
                        table_format: String, location: String, view_text: String, upgraded_to: String, storage_properties: String)

// recording error log in the database
case class TableError(catalog: String, database: String, name: String, error: String)

val failures = new ConcurrentLinkedQueue[TableError]()

def metadataForAllTables(databases: Seq[String], queue: ConcurrentLinkedQueue[TableError]): DataFrame = {
  import spark.implicits._

  val externalCatalog = spark.sharedState.externalCatalog
  databases.par.flatMap(databaseName => {
    val tables = try {
      externalCatalog.listTables(databaseName)
    } catch {
      case err: NoSuchDatabaseException =>
        failures.add(TableError("hive_metastore", databaseName, null, s"ignoring database because of ${err}"))
        null
    }
    if (tables == null) {
      failures.add(TableError("hive_metastore", databaseName, null, s"listTables returned null"))
      Seq()
    } else {
      tables.par.map(tableName => try {
        val table = externalCatalog.getTable(databaseName, tableName)
        if (table == null) {
          failures.add(TableError("hive_metastore", databaseName, tableName, s"result is null"))
          None
        } else {
          val upgraded_to = table.properties.get("upgraded_to")
          val redactedKey = "*********"

          val formattedString = table.storage.properties.map {
            case (key, value) =>
              if (key == "personalAccessToken")
                s"$key=$redactedKey(redacted)"
              else if (key.equalsIgnoreCase("password"))
                s"$key=$redactedKey(redacted)"
              else
                s"$key=$value"
          }.mkString("[", ", ", "]")

          Some(TableDetails("hive_metastore", databaseName, tableName, table.tableType.name, table.provider.orNull,
            table.storage.locationUri.map(_.toString).orNull, table.viewText.orNull,
            upgraded_to match { case Some(target) => target case None => null }, formattedString))
        }
      } catch {
        case err: Throwable =>
          failures.add(TableError("hive_metastore", databaseName, tableName, s"ignoring table because of ${err}"))
          None
      }).toList.collect {
        case Some(x) => x
      }
    }
  }).toList.toDF
}

def getInventoryDatabase(): String={
  dbutils.widgets.text("config", "./config.yml")
  val configFile = dbutils.widgets.get("config")
  val fs = FileSystem.get(new java.net.URI("file:/Workspace"), sc.hadoopConfiguration)
  val file = fs.open(new Path(configFile))
  val configContents = org.apache.commons.io.IOUtils.toString(file, java.nio.charset.StandardCharsets.UTF_8)
  val configObj = new Yaml().load(configContents).asInstanceOf[java.util.Map[String, Any]]
  val inventoryDatabase = configObj.get("inventory_database").toString()
  return inventoryDatabase

}

val inventoryDatabase = getInventoryDatabase()
var df = metadataForAllTables(spark.sharedState.externalCatalog.listDatabases().filter(_ != s"$inventoryDatabase"), failures)
var columnsToMapLower = Array("catalog","database","name","upgraded_to","storage_properties")
columnsToMapLower.map(column => {
  df = df.withColumn(column, lower(col(column)))
})
var columnsToMapUpper = Array("object_type","table_format")
columnsToMapUpper.map(column => {
  df = df.withColumn(column, upper(col(column)))
})
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(s"$inventoryDatabase.tables")
var dfTableFailures = JavaConverters.asScalaIteratorConverter(failures.iterator).asScala.toList.toDF
columnsToMapLower = Array("catalog","database","name")
columnsToMapLower.map(column => {
  dfTableFailures = dfTableFailures.withColumn(column, lower(col(column)))
})

dfTableFailures.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(s"$inventoryDatabase.table_failures")