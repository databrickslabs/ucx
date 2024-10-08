import datetime
import logging
from dataclasses import dataclass, replace
from collections.abc import Iterable, KeysView

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore import TablesCrawler

logger = logging.getLogger(__name__)


@dataclass
class TableMigrationStatus:
    src_schema: str
    src_table: str
    dst_catalog: str | None = None
    dst_schema: str | None = None
    dst_table: str | None = None
    update_ts: str | None = None

    def destination(self):
        return f"{self.dst_catalog}.{self.dst_schema}.{self.dst_table}".lower()

    @classmethod
    def from_json(cls, raw: dict[str, str]) -> "TableMigrationStatus":
        return cls(
            src_schema=raw['src_schema'],
            src_table=raw['src_table'],
            dst_catalog=raw.get('dst_catalog', None),
            dst_schema=raw.get('dst_schema', None),
            dst_table=raw.get('dst_table', None),
            update_ts=raw.get('update_ts', None),
        )


@dataclass(frozen=True)
class TableView:
    catalog: str
    schema: str
    name: str

    @property
    def key(self):
        return f"{self.catalog}.{self.schema}.{self.name}".lower()


class TableMigrationIndex:
    def __init__(self, tables: list[TableMigrationStatus]):
        self._index = {(ms.src_schema, ms.src_table): ms for ms in tables}

    def is_migrated(self, schema: str, table: str) -> bool:
        """Check if a table is migrated."""
        return self.get(schema, table) is not None

    def get(self, schema: str, table: str) -> TableMigrationStatus | None:
        """Get the migration status for a table. If the table is not migrated, return None."""
        dst = self._index.get((schema.lower(), table.lower()))
        if not dst or not dst.dst_table:
            return None
        return dst

    def snapshot(self) -> KeysView[tuple[str, str]]:
        return self._index.keys()


class TableMigrationStatusRefresher(CrawlerBase[TableMigrationStatus]):
    """Crawler to capture the migration status of tables (and views).

    Migrated tables have a property set to mark them as such; this crawler scans all tables and views, examining the
    properties for the presence of the marker.
    """

    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema, table_crawler: TablesCrawler):
        super().__init__(sbe, "hive_metastore", schema, "migration_status", TableMigrationStatus)
        self._ws = ws
        self._table_crawler = table_crawler

    def index(self, *, force_refresh: bool = False) -> TableMigrationIndex:
        return TableMigrationIndex(list(self.snapshot(force_refresh=force_refresh)))

    def get_seen_tables(self) -> dict[str, str]:
        seen_tables: dict[str, str] = {}
        for schema in self._iter_schemas():
            try:
                # ws.tables.list returns Iterator[TableInfo], so we need to convert it to a list in order to catch the exception
                tables = list(self._ws.tables.list(catalog_name=schema.catalog_name, schema_name=schema.name))
            except NotFound:
                logger.warning(
                    f"Schema {schema.catalog_name}.{schema.name} no longer exists. Skipping checking its migration status."
                )
                continue
            for table in tables:
                if not table.properties:
                    continue
                if "upgraded_from" not in table.properties:
                    continue
                if not table.full_name:
                    logger.warning(f"The table {table.name} in {schema.name} has no full name")
                    continue
                seen_tables[table.full_name.lower()] = table.properties["upgraded_from"].lower()
        return seen_tables

    def is_migrated(self, schema: str, table: str) -> bool:
        results = self._backend.fetch(
            f"SHOW TBLPROPERTIES {escape_sql_identifier(schema + '.' + table)} ('upgraded_to')"
        )
        for result in results:
            if "does not have property" in result.value:
                continue
            logger.info(f"{schema}.{table} is set as migrated")
            return True
        logger.info(f"{schema}.{table} is set as not migrated")
        return False

    def _crawl(self) -> Iterable[TableMigrationStatus]:
        all_tables = self._table_crawler.snapshot()
        reverse_seen = {v: k for k, v in self.get_seen_tables().items()}
        timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
        for table in all_tables:
            src_schema = table.database.lower()
            src_table = table.name.lower()
            table_migration_status = TableMigrationStatus(
                src_schema=src_schema,
                src_table=src_table,
                update_ts=str(timestamp),
            )
            if table.key in reverse_seen and self.is_migrated(src_schema, src_table):
                target_table = reverse_seen[table.key]
                if len(target_table.split(".")) == 3:
                    table_migration_status = replace(
                        table_migration_status,
                        dst_catalog=target_table.split(".")[0],
                        dst_schema=target_table.split(".")[1],
                        dst_table=target_table.split(".")[2],
                    )
            yield table_migration_status

    def _try_fetch(self) -> Iterable[TableMigrationStatus]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield TableMigrationStatus(*row)

    def _iter_schemas(self):
        for catalog in self._ws.catalogs.list():
            try:
                yield from self._ws.schemas.list(catalog_name=catalog.name)
            except NotFound:
                logger.warning(f"Catalog {catalog.name} no longer exists. Skipping checking its migration status.")
                continue
