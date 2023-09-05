import logging
from collections.abc import Iterator
from dataclasses import dataclass
from functools import partial

from databricks.labs.ucx.tacl._internal import CrawlerBase, SqlBackend
from databricks.labs.ucx.utils import ThreadedExecution

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

    def _sql_alter(self, catalog):
        return (
            f"ALTER {self.kind} {self.key} SET"
            f" TBLPROPERTIES ('upgraded_to' = '{catalog}.{self.database}.{self.name}');"
        )

    def _sql_external(self, catalog):
        # TODO: https://github.com/databricks/ucx/issues/106
        return (
            f"CREATE TABLE IF NOT EXISTS {catalog}.{self.database}.{self.name}"
            f" LIKE {self.key} COPY LOCATION;" + self._sql_alter(catalog)
        )

    def _sql_managed(self, catalog):
        if not self.is_delta:
            msg = f"{self.key} is not DELTA: {self.table_format}"
            raise ValueError(msg)
        return (
            f"CREATE TABLE IF NOT EXISTS {catalog}.{self.database}.{self.name}"
            f" DEEP CLONE {self.key};" + self._sql_alter(catalog)
        )

    def _sql_view(self, catalog):
        return f"CREATE VIEW IF NOT EXISTS {catalog}.{self.database}.{self.name} AS {self.view_text};"

    def uc_create_sql(self, catalog):
        if self.kind == "VIEW":
            return self._sql_view(catalog)
        elif self.location is not None:
            return self._sql_external(catalog)
        else:
            return self._sql_managed(catalog)


class TablesCrawler(CrawlerBase):
    def __init__(self, backend: SqlBackend, catalog, schema):
        """
        Initializes a TablesCrawler instance.

        Args:
            ws (WorkspaceClient): The WorkspaceClient instance.
            warehouse_id: The warehouse ID.
            catalog (str): The catalog name for the inventory persistence.
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, catalog, schema, "tables")

    def _all_databases(self) -> Iterator[str]:
        yield from self._fetch("SHOW DATABASES")

    def snapshot(self, catalog: str, database: str) -> list[Table]:
        """
        Takes a snapshot of tables in the specified catalog and database.

        Args:
            catalog (str): The catalog name.
            database (str): The database name.

        Returns:
            list[Table]: A list of Table objects representing the snapshot of tables.
        """
        return self._snapshot(
            Table, partial(self._try_load, catalog, database), partial(self._crawl, catalog, database)
        )

    def _try_load(self, catalog: str, database: str):
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(
            f'SELECT * FROM {self._full_name} WHERE catalog = "{catalog}" AND database = "{database}"'
        ):
            yield Table(*row)

    def _crawl(self, catalog: str, database: str) -> list[Table]:
        """Crawls and lists tables within the specified catalog and database.

        After performing initial scan of all tables, starts making parallel
        DESCRIBE TABLE EXTENDED queries for every table.
        """
        catalog = self._valid(catalog)
        database = self._valid(database)
        logger.debug(f"[{catalog}.{database}] listing tables")
        tasks = []
        for _, table, _is_tmp in self._fetch(f"SHOW TABLES FROM {catalog}.{database}"):
            tasks.append(partial(self._describe, catalog, database, table))
        return ThreadedExecution.gather("listing tables", tasks)

    def _describe(self, catalog: str, database: str, table: str) -> Table:
        """Fetches metadata like table type, data format, external table location,
        and the text of a view if specified for a specific table within the given
        catalog and database.
        """
        describe = {}
        full_name = f"{catalog}.{database}.{table}"
        logger.debug(f"[{full_name}] fetching table metadata")
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
