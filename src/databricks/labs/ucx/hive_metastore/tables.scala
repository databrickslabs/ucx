import spark.implicits._
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}

val PERSIST_SCHEMA = "ucx"
val DEBUG = true

case class TableDetails(catalog: String, database: String, name: String, object_type: String,
                        table_format: String, location: String, view_text: String)

// Get metadata for a given table and map it to a TableDetails case class object
def getTableDetails(db: String, t: String): Option[TableDetails] = {
  try {
    val table: CatalogTable = spark.sharedState.externalCatalog.getTable(db = db, table = t)

    if (table == null) {
      println(s"getTable('${db}.${t}') returned null")
      None
    } else {
      Some(TableDetails("hive_metastore", db, t, table.tableType.name, table.provider.getOrElse("Unknown"),
        table.storage.locationUri.getOrElse("None").toString, table.viewText.getOrElse("None"), "", // TBD: Set WS ID
        table.createTime, table.lastAccessTime))
    }
  } catch {
    case err: Throwable =>
      println(s"Got some other kind of Throwable exception, ignoring for ${db}.${t}")
      println(s"Error: ${err}")
      None
  }
}

// Retrieve metadata for a database and map it to a list of TableDetails case class objects
def getDbTables(db: String): Seq[TableDetails] = {
  val tables = spark.sharedState.externalCatalog.listTables(db)
  if (tables == null) {
    println(s"listTable('${db}') returned null")
    Seq()
  } else {
    tables.par.map(getTableDetails(db, _)).toList.collect { case Some(x) => x }
  }
}

val dbs = spark.sharedState.externalCatalog.listDatabases()

// create schema if not available
spark.sql(s"CREATE SCHEMA IF NOT EXISTS ${PERSIST_SCHEMA}")

val df = dbs.par.flatMap(getDbTables).toList.toDF

// write rows to table
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(s"${PERSIST_SCHEMA}.tables")