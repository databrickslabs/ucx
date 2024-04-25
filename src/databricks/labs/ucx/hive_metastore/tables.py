import logging
import re
import typing
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial

import sqlglot
from sqlglot import expressions
from sqlglot.expressions import LocationProperty
from sqlglot.errors import ParseError

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier

logger = logging.getLogger(__name__)


class What(Enum):
    EXTERNAL_SYNC = auto()
    EXTERNAL_HIVESERDE = auto()
    EXTERNAL_NO_SYNC = auto()
    DBFS_ROOT_DELTA = auto()
    DBFS_ROOT_NON_DELTA = auto()
    VIEW = auto()
    DB_DATASET = auto()
    UNKNOWN = auto()


class HiveSerdeType(Enum):
    PARQUET = auto()
    AVRO = auto()
    ORC = auto()
    OTHER_HIVESERDE = auto()
    NOT_HIVESERDE = auto()
    INVALID_HIVESERDE_INFO = auto()


class AclMigrationWhat(Enum):
    LEGACY_TACL = auto()
    PRINCIPAL = auto()


@dataclass
class Table:
    catalog: str
    database: str
    name: str
    object_type: str
    table_format: str

    location: str | None = None
    view_text: str | None = None
    # really means migrated_to
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
    def safe_sql_key(self) -> str:
        return escape_sql_identifier(self.key)

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return isinstance(other, Table) and self.key == other.key

    @property
    def kind(self) -> str:
        return "VIEW" if self.view_text is not None else "TABLE"

    def sql_alter_to(self, target_table_key):
        return f"ALTER {self.kind} {escape_sql_identifier(self.key)} SET TBLPROPERTIES ('upgraded_to' = '{target_table_key}');"

    def sql_alter_from(self, target_table_key, ws_id):
        return (
            f"ALTER {self.kind} {escape_sql_identifier(target_table_key)} SET TBLPROPERTIES "
            f"('upgraded_from' = '{self.key}'"
            f" , '{self.UPGRADED_FROM_WS_PARAM}' = '{ws_id}');"
        )

    def sql_unset_upgraded_to(self):
        return f"ALTER {self.kind} {escape_sql_identifier(self.key)} UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"

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
    def is_dbfs_mnt(self) -> bool:
        if not self.location:
            return False
        for dbfs_mnt_prefix in self.DBFS_ROOT_PREFIX_EXCEPTIONS:
            if self.location.startswith(dbfs_mnt_prefix):
                return True
        return False

    @property
    def is_format_supported_for_sync(self) -> bool:
        if self.table_format is None:
            return False
        return self.table_format.upper() in {"DELTA", "PARQUET", "CSV", "JSON", "ORC", "TEXT", "AVRO"}

    @property
    def is_format_supported_for_create_like(self) -> bool:
        # Based on documentation
        # https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-like.html
        if self.table_format is None:
            return False
        return self.table_format.upper() in {"DELTA", "PARQUET", "CSV", "JSON", "TEXT"}

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
        if self.kind == "TABLE" and self.table_format.upper() == "HIVE":
            return What.EXTERNAL_HIVESERDE
        if self.kind == "TABLE":
            return What.EXTERNAL_NO_SYNC
        if self.kind == "VIEW":
            return What.VIEW
        return What.UNKNOWN

    def sql_migrate_external(self, target_table_key):
        return f"SYNC TABLE {escape_sql_identifier(target_table_key)} FROM {escape_sql_identifier(self.key)};"

    def sql_migrate_ctas_external(self, target_table_key, dst_table_location) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {escape_sql_identifier(target_table_key)} "
            f"LOCATION '{dst_table_location}' "
            f"AS SELECT * FROM {self.safe_sql_key}"
        )

    def sql_migrate_ctas_managed(self, target_table_key) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {escape_sql_identifier(target_table_key)} "
            f"AS SELECT * FROM {self.safe_sql_key}"
        )

    def hiveserde_type(self, backend: SqlBackend) -> HiveSerdeType:
        if self.table_format != "HIVE":
            return HiveSerdeType.NOT_HIVESERDE
        # Extract hive serde info, ideally this should be done by table crawler.
        # But doing here to avoid breaking change to the `tables` table in the inventory schema.
        describe = {}
        for key, values, _ in backend.fetch(f"DESCRIBE TABLE EXTENDED {escape_sql_identifier(self.key)}"):
            describe[key] = values
        if not {"Serde Library", "InputFormat", "OutputFormat"} <= describe.keys():
            return HiveSerdeType.INVALID_HIVESERDE_INFO
        serde = describe["Serde Library"]
        input_format = describe["InputFormat"]
        output_format = describe["OutputFormat"]
        if self._if_parquet_serde(serde, input_format, output_format):
            return HiveSerdeType.PARQUET
        if self._if_avro_serde(serde, input_format, output_format):
            return HiveSerdeType.AVRO
        if self._if_orc_serde(serde, input_format, output_format):
            return HiveSerdeType.ORC
        return HiveSerdeType.OTHER_HIVESERDE

    def _if_parquet_serde(self, serde, input_format, output_format) -> bool:
        return (
            serde == "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            and input_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
            and output_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        )

    def _if_avro_serde(self, serde, input_format, output_format) -> bool:
        return (
            serde == "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
            and input_format == "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"
            and output_format == "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"
        )

    def _if_orc_serde(self, serde, input_format, output_format) -> bool:
        return (
            serde == "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
            and input_format == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
            and output_format == "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
        )

    def sql_migrate_external_hiveserde_in_place(
        self,
        catalog_name,
        dst_schema,
        dst_table,
        backend: SqlBackend,
        hiveserde_type: HiveSerdeType,
        replace_table_location: str | None = None,
    ) -> str | None:
        # "PARQUET", "AVRO", "ORC" can be migrated with "SHOW CREATE TABLE..." DDL
        if hiveserde_type in [HiveSerdeType.PARQUET, HiveSerdeType.AVRO, HiveSerdeType.ORC]:
            return self._ddl_show_create_table(backend, catalog_name, dst_schema, dst_table, replace_table_location)

        # "TEXTFILE" hiveserde needs extra handling on preparing the DDL
        # TODO: add support for "TEXTFILE" hiveserde, when the data can be parsed as Spark CSV datasource

        # "JSON", "CSV" hiveserde need extra handling on preparing the DDL
        # TODO: DBR does not bundle the jars for "JSON", "CSV" hiveserde, it's unlikely we see those tables.
        #  Although it's possible that users has the jars installed as cluster library and use those tables in Databricks,
        #  we hold off the implementation for now until we see the real use case.
        return None

    def _ddl_show_create_table(
        self, backend: SqlBackend, catalog_name, dst_schema, dst_table, replace_location
    ) -> str | None:
        # get raw DDL from "SHOW CREATE TABLE..."
        createtab_stmt = next(backend.fetch(f"SHOW CREATE {self.kind} {self.safe_sql_key};"))["createtab_stmt"]
        # parse the DDL and replace the old table name with the new UC table name
        try:
            statements = sqlglot.parse(createtab_stmt)
        except (ValueError, ParseError):
            logger.exception(f"Exception when parsing 'SHOW CREATE TABLE' DDL for {self.key}")
            return None

        statement = statements[0]
        if not statement:
            logger.error(f"sqlglot parsed none statement from 'SHOW CREATE TABLE' DDL for {self.key}")
            return None

        src_table = statement.find(expressions.Table)
        if not src_table:
            logger.error(f"sqlglot failed to extract table object from parsed DDL for {self.key}")
            return None
        new_table = expressions.Table(catalog=catalog_name, db=dst_schema, this=dst_table)
        src_table.replace(new_table)

        if replace_location:
            # replace dbfs mnt in ddl if any
            mnt_loc = statement.find(LocationProperty)
            if not mnt_loc:
                logger.error(f"sqlglot failed to extract table location object from parsed DDL for {self.key}")
                return None
            new_loc = LocationProperty(this=f"'{replace_location}'")
            mnt_loc.replace(new_loc)

        new_sql = statement.sql('databricks')
        return new_sql

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
            try:
                table_rows = self._fetch(
                    f"SHOW TABLES FROM {escape_sql_identifier(catalog)}.{escape_sql_identifier(database)}"
                )
                for _, table, _is_tmp in table_rows:
                    tasks.append(partial(self._describe, catalog, database, table))
            except NotFound:
                # This make the integration test more robust as many test schemas are being created and deleted quickly.
                # In case a schema is deleted, StatementExecutionBackend returns empty result but RuntimeBackend raises NotFound
                logger.warning(f"Schema {catalog}.{database} no longer existed")
                continue
        catalog_tables, errors = Threads.gather(f"listing tables in {catalog}", tasks)
        if len(errors) > 0:
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
                storage_properties=self._parse_table_props(
                    describe.get("Storage Properties", "").lower()
                ),  # type: ignore[arg-type]
                is_partitioned="# Partition Information" in describe,
            )
        except NotFound:
            # This make the integration test more robust as many test schemas are being created and deleted quickly.
            logger.warning(f"Schema {catalog}.{database} no longer existed")
            return None
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"Couldn't fetch information for table {full_name} : {e}")
            return None
