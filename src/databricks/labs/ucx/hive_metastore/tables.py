import logging
import re
import typing
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial

from databricks.labs.blueprint.parallel import Threads

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.framework.utils import escape_sql_identifier

logger = logging.getLogger(__name__)


class What(Enum):
    EXTERNAL_SYNC = auto()
    EXTERNAL_NO_SYNC = auto()
    DBFS_ROOT_DELTA = auto()
    DBFS_ROOT_NON_DELTA = auto()
    VIEW = auto()
    DB_DATASET = auto()
    UNKNOWN = auto()


@dataclass
class Table:
    catalog: str
    database: str
    name: str
    object_type: str
    table_format: str

    location: str | None = None
    view_text: str | None = None
    upgraded_to: str | None = None

    storage_properties: str | None = None
    is_partitioned: bool = False

    DBFS_ROOT_PREFIXES: typing.ClassVar[list[str]] = [
        "/dbfs/",
        "dbfs:/",
    ]

    DBFS_ROOT_PREFIX_EXCEPTIONS: typing.ClassVar[list[str]] = [
        "/dbfs/mnt",
        "dbfs:/mnt",
    ]

    DBFS_DATABRICKS_DATASETS: typing.ClassVar[list[str]] = [
        "/dbfs/databricks-datasets",
        "dbfs:/databricks-datasets",
    ]

    UPGRADED_FROM_WS_PARAM: typing.ClassVar[str] = "upgraded_from_workspace_id"

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

    def sql_alter_to(self, target_table_key):
        return f"ALTER {self.kind} {self.key} SET TBLPROPERTIES ('upgraded_to' = '{target_table_key}');"

    def sql_alter_from(self, target_table_key, ws_id):
        return (
            f"ALTER {self.kind} {target_table_key} SET TBLPROPERTIES "
            f"('upgraded_from' = '{self.key}'"
            f" , '{self.UPGRADED_FROM_WS_PARAM}' = '{ws_id}');"
        )

    def sql_unset_upgraded_to(self):
        return f"ALTER {self.kind} {self.key} UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"

    @property
    def is_dbfs_root(self) -> bool:
        if not self.location:
            return False
        for prefix in self.DBFS_ROOT_PREFIXES:
            if not self.location.startswith(prefix):
                continue
            for exception in self.DBFS_ROOT_PREFIX_EXCEPTIONS:
                if self.location.startswith(exception):
                    return False
            for db_datasets in self.DBFS_DATABRICKS_DATASETS:
                if self.location.startswith(db_datasets):
                    return False
            return True
        return False

    @property
    def is_format_supported_for_sync(self) -> bool:
        if self.table_format is None:
            return False
        return self.table_format.upper() in {"DELTA", "PARQUET", "CSV", "JSON", "ORC", "TEXT"}

    @property
    def is_databricks_dataset(self) -> bool:
        if not self.location:
            return False
        for db_datasets in self.DBFS_DATABRICKS_DATASETS:
            if self.location.startswith(db_datasets):
                return True
        return False

    @property
    def what(self) -> What:
        if self.is_databricks_dataset:
            return What.DB_DATASET
        if self.is_dbfs_root and self.table_format == "DELTA":
            return What.DBFS_ROOT_DELTA
        if self.is_dbfs_root:
            return What.DBFS_ROOT_NON_DELTA
        if self.kind == "TABLE" and self.is_format_supported_for_sync:
            return What.EXTERNAL_SYNC
        if self.kind == "TABLE":
            return What.EXTERNAL_NO_SYNC
        if self.kind == "VIEW":
            return What.VIEW
        return What.UNKNOWN

    def sql_migrate_external(self, target_table_key):
        return f"SYNC TABLE {escape_sql_identifier(target_table_key)} FROM {escape_sql_identifier(self.key)};"

    def sql_migrate_dbfs(self, target_table_key):
        if not self.is_delta:
            msg = f"{self.key} is not DELTA: {self.table_format}"
            raise ValueError(msg)
        return f"CREATE TABLE IF NOT EXISTS {escape_sql_identifier(target_table_key)} DEEP CLONE {escape_sql_identifier(self.key)};"

    def sql_migrate_view(self, target_table_key):
        return f"CREATE VIEW IF NOT EXISTS {escape_sql_identifier(target_table_key)} AS {self.view_text};"


@dataclass
class TableError:
    catalog: str
    database: str
    name: str | None = None
    error: str | None = None


@dataclass
class MigrationCount:
    database: str
    what_count: dict[What, int]


class TablesCrawler(CrawlerBase):
    def __init__(self, backend: SqlBackend, schema, include_databases: list[str] | None = None):
        """
        Initializes a TablesCrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "tables", Table)
        self._include_database = include_databases

    def _all_databases(self) -> list[str]:
        if not self._include_database:
            return [row[0] for row in self._fetch("SHOW DATABASES")]
        return self._include_database

    def snapshot(self) -> list[Table]:
        """
        Takes a snapshot of tables in the specified catalog and database.

        Returns:
            list[Table]: A list of Table objects representing the snapshot of tables.
        """
        return self._snapshot(partial(self._try_load), partial(self._crawl))

    @staticmethod
    def _parse_table_props(tbl_props: str) -> dict:
        pattern = r"([^,\[\]]+)=([^,\[\]]+)"
        key_value_pairs = re.findall(pattern, tbl_props)
        # Convert key-value pairs to dictionary
        return dict(key_value_pairs)

    @staticmethod
    def parse_database_props(tbl_props: str) -> dict:
        pattern = r"([^,^\(^\)\[\]]+),([^,^\(^\)\[\]]+)"
        key_value_pairs = re.findall(pattern, tbl_props)
        # Convert key-value pairs to dictionary
        return dict(key_value_pairs)

    def _try_load(self) -> Iterable[Table]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Table(*row)

    def _crawl(self) -> Iterable[Table]:
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
        for database in self._all_databases():
            logger.debug(f"[{catalog}.{database}] listing tables")
            for _, table, _is_tmp in self._fetch(
                f"SHOW TABLES FROM {escape_sql_identifier(catalog)}.{escape_sql_identifier(database)}"
            ):
                tasks.append(partial(self._describe, catalog, database, table))
        catalog_tables, errors = Threads.gather(f"listing tables in {catalog}", tasks)
        if len(errors) > 0:
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            logger.error(f"Detected {len(errors)} while scanning tables in {catalog}")
        return catalog_tables

    @staticmethod
    def _safe_norm(value: str | None, *, lower: bool = True) -> str | None:
        if not value:
            return None
        if lower:
            return value.lower()
        return value.upper()

    def _describe(self, catalog: str, database: str, table: str) -> Table | None:
        """Fetches metadata like table type, data format, external table location,
        and the text of a view if specified for a specific table within the given
        catalog and database.
        """
        full_name = f"{catalog}.{database}.{table}"
        try:
            logger.debug(f"[{full_name}] fetching table metadata")
            describe = {}
            for key, value, _ in self._fetch(f"DESCRIBE TABLE EXTENDED {escape_sql_identifier(full_name)}"):
                describe[key] = value
            return Table(
                catalog=catalog.lower(),
                database=database.lower(),
                name=table.lower(),
                object_type=describe.get("Type", "UNKNOWN").upper(),
                table_format=describe.get("Provider", "UNKNOWN").upper(),
                location=describe.get("Location", None),
                view_text=describe.get("View Text", None),
                upgraded_to=self._parse_table_props(describe.get("Table Properties", "").lower()).get(
                    "upgraded_to", None
                ),
                storage_properties=self._parse_table_props(describe.get("Storage Properties", "").lower()),  # type: ignore[arg-type]
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            logger.error(f"Couldn't fetch information for table {full_name} : {e}")
            return None
