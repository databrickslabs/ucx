import logging
from collections.abc import Iterator
from dataclasses import dataclass
from functools import partial

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.framework.parallel import ThreadedExecution
from databricks.labs.ucx.mixins.sql import Row

logger = logging.getLogger(__name__)

@dataclass
class SyncStatus:
    source_schema: str
    source_name: str
    source_type:str
    target_catalog:str
    target_schema:str
    target_name:str
    status_code:str
    description:str

@dataclass
class Table:
    catalog: str
    database: str
    name: str
    object_type: str
    table_format: str

    location: str = None
    view_text: str = None

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

    def sql_alter(self, catalog):
        return (
            f"ALTER {self.kind} {self.key} SET"
            f" TBLPROPERTIES ('upgraded_to' = '{catalog}.{self.database}.{self.name}');"
        )

    def _sql_external(self, catalog):
        return f"SYNC TABLE {catalog}.{self.database}.{self.name} FROM {self.key}; "

    def _sql_managed(self, catalog):
        if not self.is_delta:
            msg = f"{self.key} is not DELTA: {self.table_format}"
            raise ValueError(msg)
        return (
            f"CREATE TABLE IF NOT EXISTS {catalog}.{self.database}.{self.name}"
            f" DEEP CLONE {self.key} "
        )

    def _sql_view(self, catalog):
        return f"CREATE VIEW IF NOT EXISTS {catalog}.{self.database}.{self.name} AS {self.view_text};"

    def uc_create_sql(self, catalog):
        if self.kind == "VIEW":
            return self._sql_view(catalog)
        elif self.object_type == "EXTERNAL":
            return self._sql_external(catalog)
        else:
            return self._sql_managed(catalog)


class TablesCrawler(CrawlerBase):
    def __init__(self, backend: SqlBackend, schema):
        """
        Initializes a TablesCrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "tables")
        self._inventory_database = schema

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
            )
        except Exception as e:
            logger.error(f"Couldn't fetch information for table {full_name} : {e}")
            return None

    def migrate_tables(self, target_catalog):
        tables = self._fetch_tables()
        for table in tables:
            self._migrate_table(target_catalog, table)

    def _migrate_table(self, target_catalog, table):
        try:
            sql = table.uc_create_sql(target_catalog)
            logger.debug(f"Migrating table {table.key} to using SQL query: {sql}")
            if table.object_type == "EXTERNAL":
                sync_status = SyncStatus(*list(self._backend.fetch(sql))[0])
                if sync_status.status_code != "SUCCESS":
                    logger.error(f"Could not sync external table {table.key} to {target_catalog}.{table.database} "
                                 f"because: {sync_status.status_code} {sync_status.description}")
                else:
                    self._backend.execute(table.sql_alter(target_catalog))
            else:
                self._backend.execute(sql)
                self._backend.execute(table.sql_alter(target_catalog))
        except Exception as e:
            logger.error(f"Could not create table {table.name} because: {e}")

    def _fetch_tables(self):
        try:
            tables = []
            for row in self._backend.fetch(f"SELECT * FROM hive_metastore.{self._inventory_database}.tables"):
                tables.append(Table(*row))
            logger.debug(f"Found {len(tables)} tables to migrate")
            return tables
        except Exception as e:
            logger.error(f"Could not query inventory table : {e}")
            raise e

