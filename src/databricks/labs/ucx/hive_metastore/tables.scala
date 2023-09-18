import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.DataFrame

// must follow the same structure as databricks.labs.ucx.hive_metastore.tables.Table
case class TableDetails(catalog: String, database: String, name: String, object_type: String,
                        table_format: String, location: String, view_text: String)

def metadataForAllTables(databases: Seq[String]): DataFrame = {
  import spark.implicits._

  val externalCatalog = spark.sharedState.externalCatalog
  databases.par.flatMap(databaseName => {
    val tables = externalCatalog.listTables(databaseName)
    if (tables == null) {
      println(s"[WARN][${databaseName}] listTables returned null")
      Seq()
    } else {
      tables.par.map(tableName => try {
        val table = externalCatalog.getTable(databaseName, tableName)
        if (table == null) {
          println(s"[WARN][${databaseName}.${tableName}] result is null")
          None
        } else {
          Some(TableDetails("hive_metastore", databaseName, tableName, table.tableType.name, table.provider.orNull,
            table.storage.locationUri.map(_.toString).orNull, table.viewText.orNull))
        }
      } catch {
        case err: Throwable =>
          println(s"[ERROR][${databaseName}.${tableName}] ignoring table because of ${err}")
          None
      }).toList.collect {
        case Some(x) => x
      }
    }
  }).toList.toDF
}

dbutils.widgets.text("inventory_database", "ucx")
val inventoryDatabase = dbutils.widgets.get("inventory_database")

val df = metadataForAllTables(spark.sharedState.externalCatalog.listDatabases())
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(s"$inventoryDatabase.tables")