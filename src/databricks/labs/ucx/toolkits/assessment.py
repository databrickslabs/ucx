
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language

from databricks.labs.ucx.providers.client import ImprovedWorkspaceClient
from databricks.labs.ucx.providers.mixins.compute import CommandExecutor
from databricks.labs.ucx.tacl._internal import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)


class Assessment:
    def __init__(self, ws: WorkspaceClient, inventory_catalog, inventory_schema, warehouse_id=None):
        self._ws = ws
        self._inventory_catalog = inventory_catalog
        self._inventory_schema = inventory_schema
        self._warehouse_id = warehouse_id

    @staticmethod
    def _verify_ws_client(w: ImprovedWorkspaceClient):
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    def table_inventory(self):
        commands = CommandExecutor(self._ws, language=Language.SCALA)

        command_output = commands.run(
            f"""
             import org.apache.spark.sql.catalyst.catalog.{{CatalogTable, CatalogTableType}};
             import org.apache.spark.sql.catalyst.TableIdentifier;
             import java.io.{{FileWriter, BufferedWriter, File}};
             val bw = new BufferedWriter(new FileWriter(new File("/tmp/metastore_schema.csv"), true));

             bw.write("db,table,format,type,table_location,created_version,created_time,last_access,lib,inputformat,outputformat\\n");
             val dbs = spark.sharedState.externalCatalog.listDatabases();
             for( db <- dbs) {{
               //println(s"database: ${{db}}")
               val tables = spark.sharedState.externalCatalog.listTables(db);
               for (t <- tables) {{
                 try {{
                   //println(s"table: ${{t}}")
                   val table: CatalogTable = spark.sharedState.externalCatalog.getTable(db = db, table = t);
                   val row = s"${{db}},${{t}},${{table.provider.getOrElse("Unknown")}},${{table.tableType.name}},${{table.storage.locationUri.getOrElse("None")}},${{table.createVersion}},${{table.createTime}},${{table.lastAccessTime}},${{table.storage.serde.getOrElse("Unknown")}},${{table.storage.inputFormat.getOrElse("Unknown")}},${{table.storage.outputFormat.getOrElse("Unknown")}}\\n";
                   bw.write(row);
                 }} catch {{
                   case e: Exception => bw.write(s"${{db}},${{t}},Unknown,Unknown,NONE,,,,,,,\\n");
                 }}
               }}

             }}

             bw.close;
             spark.sql("create catalog if not exists {self._inventory_catalog}");
             spark.sql("create database if not exists {self._inventory_catalog}.{self._inventory_schema}");
             val tables_df = spark.read.option("header","true").option("inferSchema","true").csv("/tmp/metastore_schema.csv");
             tables_df.write.mode("overwrite").saveAsTable("{self._inventory_catalog}.{self._inventory_schema}.hms_tables");
            """
        )
        print(command_output)

    def external_locations(self):
        pass

    @staticmethod
    def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
        if warehouse_id is None:
            return RuntimeBackend()
        return StatementExecutionBackend(ws, warehouse_id)
