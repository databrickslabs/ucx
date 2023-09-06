import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType};

import org.apache.spark.sql.catalyst.TableIdentifier;
import java.io.{FileWriter, BufferedWriter, File};

val schema = "SCHEMA"
val bw = new BufferedWriter(new FileWriter(new File("/tmp/metastore_schema.csv"), true));

bw.write("db,table,format,type,table_location,created_version,created_time,last_access,lib,inputformat,outputformat\n")
val dbs = spark.sharedState.externalCatalog.listDatabases()
for( db <- dbs) {
  //println(s"database: ${db}")
  val tables = spark.sharedState.externalCatalog.listTables(db)
  for (t <- tables) {
    try {
      //println(s"table: ${t}")
      val table: CatalogTable = spark.sharedState.externalCatalog.getTable(db = db, table = t)
      val row = s"${db},${t},${table.provider.getOrElse("Unknown")},${table.tableType.name},${table.storage.locationUri.getOrElse("None")},${table.createVersion},${table.createTime},${table.lastAccessTime},${table.storage.serde.getOrElse("Unknown")},${table.storage.inputFormat.getOrElse("Unknown")},${table.storage.outputFormat.getOrElse("Unknown")}\n"
      bw.write(row)
    } catch {
      case e: Exception => bw.write(s"${db},${t},Unknown,Unknown,NONE,,,,,,,\n")
    }
  }

}

bw.close;

dbutils.fs.cp("file:///tmp/metastore_schema.csv","dbfs:/tmp/uc_assessment/hms/")
spark.sql(s"create database if not exists ${schema}")
val tables_df=spark.read.option("header","true").csv("/tmp/uc_assessment/hms/metastore_schema.csv")
tables_df.write.mode("overwrite").saveAsTable(s"${schema}.hms_tables");