import logging
from collections.abc import Iterator
from dataclasses import dataclass
from functools import partial

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.framework.parallel import ThreadedExecution
from databricks.labs.ucx.mixins.sql import Row

logger = logging.getLogger(__name__)


@dataclass
class Table:
    catalog: str
    database: str
    name: str
    object_type: str
    table_format: str

    location: str = None
    view_text: str = None
    table_properties: str = None

    @property
    def is_delta(self) -> bool:
        if self.table_format is None:
            return False
        return self.table_format.upper() == "DELTA"

    @property
    def key(self) -> str:
        return f"{self.catalog}.{self.database}.{self.name}".lower()

    @property
    def kind(self) -> str:
        return "VIEW" if self.view_text is not None else "TABLE"

    def _sql_external(self, catalog):
        return f"SYNC TABLE {catalog}.{self.database}.{self.name} FROM {self.key};"

    def _sql_managed(self, catalog):
        if not self.is_delta:
            msg = f"{self.key} is not DELTA: {self.table_format}"
            raise ValueError(msg)
        return f"CREATE TABLE IF NOT EXISTS {catalog}.{self.database}.{self.name} DEEP CLONE {self.key};"

    def _sql_view(self, catalog):
        return f"CREATE VIEW IF NOT EXISTS {catalog}.{self.database}.{self.name} AS {self.view_text};"

    def uc_create_sql(self, catalog):
        if self.kind == "VIEW":
            return self._sql_view(catalog)
        elif self.object_type == "EXTERNAL":
            return self._sql_external(catalog)
        else:
            return self._sql_managed(catalog)

    def sql_alter_to(self, catalog):
        return (
            f"ALTER {self.kind} {self.key} SET"
            f" TBLPROPERTIES ('upgraded_to' = '{catalog}.{self.database}.{self.name}');"
        )

    def sql_alter_from(self, catalog):
        return (
            f"ALTER {self.kind} {catalog}.{self.database}.{self.name} SET"
            f" TBLPROPERTIES ('upgraded_from' = '{self.key}');"
        )


class TablesCrawler(CrawlerBase):
    def __init__(self, backend: SqlBackend, schema):
        """
        Initializes a TablesCrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "tables")

    def _all_databases(self) -> Iterator[Row]:
        yield from self._fetch("SHOW DATABASES")

    def snapshot(self) -> list[Table]:
        """
        Takes a snapshot of tables in the specified catalog and database.

        Returns:
            list[Table]: A list of Table objects representing the snapshot of tables.
        """
        return self._snapshot(partial(self._try_load), partial(self._crawl))

    def _try_load(self):
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {self._full_name}"):
            yield Table(*row)

    def _crawl(self) -> list[Table]:
        """Crawls and lists tables within the specified catalog and database.

        After performing initial scan of all tables, starts making parallel
        DESCRIBE TABLE EXTENDED queries for every table.

        Production tasks would most likely be executed through `tables.scala`
        within `crawl_tables` task due to `spark.sharedState.externalCatalog`
        lower-level APIs not requiring a roundtrip to storage, which is not
        possible for Azure storage with credentials supplied through Spark
        conf (see https://github.com/databrickslabs/ucx/issues/249).

        See also https://github.com/databrickslabs/ucx/issues/247
        """
        tasks = []
        catalog = "hive_metastore"
        for (database,) in self._all_databases():
            logger.debug(f"[{catalog}.{database}] listing tables")
            for _, table, _is_tmp in self._fetch(f"SHOW TABLES FROM {catalog}.{database}"):
                tasks.append(partial(self._describe, catalog, database, table))
        results = ThreadedExecution.gather(f"listing tables in {catalog}", tasks)
        return [x for x in results if x is not None]

    def _describe(self, catalog: str, database: str, table: str) -> Table | None:
        """Fetches metadata like table type, data format, external table location,
        and the text of a view if specified for a specific table within the given
        catalog and database.
        """
        full_name = f"{catalog}.{database}.{table}"
        try:
            logger.debug(f"[{full_name}] fetching table metadata")
            describe = {}
            for key, value, _ in self._fetch(f"DESCRIBE TABLE EXTENDED {full_name}"):
                describe[key] = value
            return Table(
                catalog=describe["Catalog"],
                database=database,
                name=table,
                object_type=describe["Type"],
                table_format=describe.get("Provider", "").upper(),
                location=describe.get("Location", None),
                view_text=describe.get("View Text", None),
                table_properties=describe.get("Table Properties", None),
            )
        except Exception as e:
            logger.error(f"Couldn't fetch information for table {full_name} : {e}")
            return None


class TablesMigrate:
    def __init__(
        self,
        tc: TablesCrawler,
        ws: WorkspaceClient,
        backend: SqlBackend,
        inventory_database: str,
        default_catalog=None,
        database_to_catalog_mapping: dict[str, str] | None = None,
    ):
        self._tc = tc
        self._backend = backend
        self._ws = ws
        self._inventory_database = inventory_database
        self._database_to_catalog_mapping = database_to_catalog_mapping
        self._seen_tables = {}
        self._default_catalog = self._init_default_catalog(default_catalog)

    @staticmethod
    def _init_default_catalog(default_catalog):
        if default_catalog:
            return default_catalog
        else:
            return "ucx_default"  # TODO : Fetch current workspace name and append it to the default catalog.

    def migrate_tables(self):
        tasks = []
        for table in self._tc.snapshot():
            if self._database_to_catalog_mapping:
                tasks.append(partial(self._migrate_table, self._database_to_catalog_mapping[table.database], table))
            else:
                tasks.append(partial(self._migrate_table, self._default_catalog, table))
        ThreadedExecution.gather("migrate tables", tasks)

    def _migrate_table(self, target_catalog, table):
        try:
            sql = table.uc_create_sql(target_catalog)
            logger.debug(f"Migrating table {table.key} to using SQL query: {sql}")

            if table.object_type == "MANAGED" and "upgraded_to" not in table.table_properties:
                self._backend.execute(sql)
                self._backend.execute(table.sql_alter_to(target_catalog))
                self._backend.execute(table.sql_alter_from(target_catalog))
            else:
                logger.info(f"Table {table.key} is a {table.object_type} and is not supported for migration yet ")
        except Exception as e:
            logger.error(f"Could not create table {table.name} because: {e}")
