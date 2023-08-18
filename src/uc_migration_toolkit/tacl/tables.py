from dataclasses import dataclass
from functools import partial
from typing import Iterator

from databricks.sdk import WorkspaceClient

from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.tacl._internal import CrawlerBase
from uc_migration_toolkit.utils import ThreadedExecution


@dataclass
class Table:
    catalog: str
    database: str
    name: str
    object_type: str
    format: str

    location: str = None
    view_text: str = None

    @property
    def is_delta(self) -> bool:
        if self.format is None: return False
        return self.format.lower() == 'delta'

    @property
    def key(self) -> str:
        return f'{self.catalog}.{self.database}.{self.name}'.lower()

    @property
    def kind(self) -> str:
        return 'VIEW' if self.view_text != '' else 'TABLE'

    def _sql_alter(self, catalog):
        return f'ALTER {self.kind} hive_metastore.{self.database}.{self.name} SET' \
               f" TBLPROPERTIES ('upgraded_to' = '{catalog}.{self.database}.{self.name}');"

    def _sql_external(self, catalog):
        return f'CREATE TABLE IF NOT EXISTS {catalog}.{self.database}.{self.name}' \
               f' LIKE hive_metastore.{self.database}.{self.name} COPY LOCATION;' \
               + self._sql_alter(catalog)

    def _sql_managed(self, catalog):
        if not self.is_delta:
            raise ValueError(f'{self.key} is not DELTA: {self.format}')
        return f'CREATE TABLE IF NOT EXISTS {catalog}.{self.database}.{self.name}' \
               f' DEEP CLONE hive_metastore.{self.database}.{self.name};' \
               + self._sql_alter(catalog)

    def _sql_view(self, catalog):
        return f'CREATE VIEW IF NOT EXISTS {catalog}.{self.database}.{self.name}' \
               f' AS {self.view_text};'


class TablesCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, warehouse_id, catalog, schema):
        super().__init__(ws, warehouse_id, catalog, schema, 'tables')
        self._warehouse_id = warehouse_id
        self._ws = ws

    def _all_databases(self) -> Iterator[str]:
        yield from self._fetch("SHOW DATABASES")

    def snapshot(self, catalog: str, database: str) -> list[Table]:
        return self._snapshot(Table,
                              partial(self._try_load, catalog, database),
                              partial(self._crawl, catalog, database))

    def _try_load(self, catalog: str, database: str):
        for row in self._fetch(
                f'SELECT * FROM {self._full_name} WHERE catalog = "{catalog}" AND database = "{database}"'):
            yield Table(*row)

    def _crawl(self, catalog: str, database: str) -> list[Table]:
        catalog = self._valid(catalog)
        database = self._valid(database)
        logger.debug(f'[{catalog}.{database}] listing tables')
        tasks = []
        for _, table, is_tmp in self._fetch(f"SHOW TABLES FROM {catalog}.{database}"):
            tasks.append(partial(self._describe, catalog, database, table))
        return ThreadedExecution.gather('listing tables', tasks)

    def _describe(self, catalog: str, database: str, table: str) -> Table:
        describe = {}
        full_name = f'{catalog}.{database}.{table}'
        logger.debug(f'[{full_name}] fetching table metadata')
        for key, value, _ in self._fetch(f"DESCRIBE TABLE EXTENDED {full_name}"):
            describe[key] = value
        return Table(
            catalog=describe['Catalog'],
            database=database,
            name=table,
            object_type=describe["Type"],
            format=describe.get("Provider", ''),
            location=describe.get("Location", ''),
            view_text=describe.get("View Text", ''),
        )


