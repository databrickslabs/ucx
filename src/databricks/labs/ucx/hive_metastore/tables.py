import logging
import re
import typing
from collections.abc import Iterable, Iterator, Collection
from dataclasses import dataclass
from enum import Enum, auto
from functools import cached_property, partial

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
    TABLE_IN_MOUNT = auto()
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
        if self.is_table_in_mount:
            return f"{self.catalog}.{self.database}.{self.location}".lower()
        return f"{self.catalog}.{self.database}.{self.name}".lower()

    @property
    def safe_sql_key(self) -> str:
        return escape_sql_identifier(self.key)

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.database}.{self.name}"

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return isinstance(other, Table) and self.key == other.key

    @property
    def kind(self) -> str:
        return "VIEW" if self.view_text is not None else "TABLE"

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
    def is_databricks_dataset(self) -> bool:
        if not self.location:
            return False
        for db_datasets in self.DBFS_DATABRICKS_DATASETS:
            if self.location.startswith(db_datasets):
                return True
        return False

    @property
    def is_table_in_mount(self) -> bool:
        return self.database.startswith("mounted_") and self.is_delta

    @property
    def what(self) -> What:
        if self.is_databricks_dataset:
            return What.DB_DATASET
        if self.is_table_in_mount:
            return What.TABLE_IN_MOUNT
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

    def sql_migrate_table_in_mount(self, target_table_key: str, table_schema: Iterator[typing.Any]):
        fields = []
        partitioned_fields = []
        next_fields_are_partitioned = False
        for key, value, _ in table_schema:
            if key == "# Partition Information":
                continue
            if key == "# col_name":
                next_fields_are_partitioned = True
                continue
            if next_fields_are_partitioned:
                partitioned_fields.append(escape_sql_identifier(key, maxsplit=0))
            else:
                fields.append(f"{escape_sql_identifier(key, maxsplit=0)} {value}")

        partitioned_str = ""
        if partitioned_fields:
            partitioning_columns = ", ".join(partitioned_fields)
            partitioned_str = f"PARTITIONED BY ({partitioning_columns})"
        schema = ", ".join(fields)

        return f"CREATE TABLE IF NOT EXISTS {escape_sql_identifier(target_table_key)} ({schema}) {partitioned_str} LOCATION '{self.location}';"


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


class TablesCrawler(CrawlerBase[Table]):
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

    def load_one(self, schema_name: str, table_name: str) -> Table | None:
        query = f"SELECT * FROM {escape_sql_identifier(self.full_name)} WHERE database='{schema_name}' AND name='{table_name}' LIMIT 1"
        for row in self._fetch(query):
            return Table(*row)
        return None

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

    def _try_fetch(self) -> Iterable[Table]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Table(*row)

    def _show_tables_in_database(self, catalog: str, database: str) -> list[Table]:
        table_rows: list[Table] = []
        try:
            logger.debug(f"[{catalog}.{database}] listing tables and views")
            for row in self._fetch(
                f"SHOW TABLES FROM {escape_sql_identifier(catalog)}.{escape_sql_identifier(database)}"
            ):
                table_rows.append(
                    Table(
                        catalog=catalog,
                        database=database,
                        name=row[1],
                        object_type='UNKNOWN',
                        table_format='UNKNOWN',
                    )
                )
        except NotFound:
            # This make the integration test more robust as many test schemas are being created and deleted quickly.
            # In case a schema is deleted, StatementExecutionBackend returns empty result but RuntimeBackend
            # raises NotFound
            logger.warning(f"Schema {catalog}.{database} no longer exists")
        return table_rows

    def _crawl(self) -> Iterable[Table]:
        """Crawls and lists tables within the specified catalog and database.

        After performing initial scan of all tables, starts making parallel
        DESCRIBE TABLE EXTENDED queries for every table.

        Production tasks would most likely be executed through FasterTableScanCrawler
        within `crawl_tables` task due to `spark.sharedState.externalCatalog`
        lower-level APIs not requiring a roundtrip to storage, which is not
        possible for Azure storage with credentials supplied through Spark
        conf (see https://github.com/databrickslabs/ucx/issues/249).

        FasterTableScanCrawler uses the _jsparkSession to utilize faster scanning with Scala APIs.

        See also https://github.com/databrickslabs/ucx/issues/247
        """
        tasks = []
        catalog = "hive_metastore"
        table_names = [partial(self._show_tables_in_database, catalog, database) for database in self._all_databases()]
        for batch in Threads.strict('listing tables', table_names):
            for table in batch:
                tasks.append(partial(self._describe, table.catalog, table.database, table.name))
        catalog_tables, errors = Threads.gather(f"describing tables in {catalog}", tasks)
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


class FasterTableScanCrawler(CrawlerBase[Table]):
    """
    FasterTableScanCrawler is a specialized version of TablesCrawler that uses spark._jsparkSession to utilize
    faster scanning with Scala APIs.

    For integration testing, FasterTableScanCrawler is tested using the larger assessment test rather than
    just the class. Testing the class individually would require utilizing a remote spark connection with the
    Databricks workspace.
    """

    def __init__(self, backend: SqlBackend, schema, include_databases: list[str] | None = None):
        self._backend = backend
        self._include_database = include_databases

        # pylint: disable-next=import-error,import-outside-toplevel
        from pyspark.sql.session import SparkSession  # type: ignore[import-not-found]

        super().__init__(backend, "hive_metastore", schema, "tables", Table)
        self._spark = SparkSession.builder.getOrCreate()

    @cached_property
    def _external_catalog(self):
        return self._spark._jsparkSession.sharedState().externalCatalog()  # pylint: disable=protected-access

    def _iterator(self, result: typing.Any) -> Iterator:
        iterator = result.iterator()
        while iterator.hasNext():
            yield iterator.next()

    @staticmethod
    def _option_as_python(scala_option: typing.Any):
        return scala_option.get() if scala_option.isDefined() else None

    def _all_databases(self) -> list[str]:
        try:
            if not self._include_database:
                return list(self._iterator(self._external_catalog.listDatabases()))
            return self._include_database
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.error(f"failed-table-crawl: listing databases -> catalog : {err}", exc_info=True)
            return []

    def _list_tables(self, database: str) -> list[str]:
        try:
            return list(self._iterator(self._external_catalog.listTables(database)))
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning(f"failed-table-crawl: listing tables from database -> {database} : {err}", exc_info=True)
            return []

    def _try_fetch(self) -> Iterable[Table]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Table(*row)

    @staticmethod
    def _format_properties_list(properties_list: list) -> str:
        if len(properties_list) == 0:
            return ""
        formatted_items: list[str] = []
        for each_property in properties_list:
            key = each_property.productElement(0)
            value = each_property.productElement(1)

            redacted_key = "*******"

            if key == "personalAccessToken" or key.lower() == "password":
                value = redacted_key
            elif value is None:
                value = "None"

            formatted_items.append(f"{key}={value}")
        return f"[{', '.join(formatted_items)}]"

    def _describe(self, catalog, database, table) -> Table | None:
        """Fetches metadata like table type, data format, external table location,
        and the text of a view if specified for a specific table within the given
        catalog and database.
        """
        full_name = f"{catalog}.{database}.{table}"
        if catalog != "hive_metastore":
            msg = f"Only tables in the hive_metastore catalog can be described: {full_name}"
            raise ValueError(msg)
        logger.debug(f"Fetching metadata for table: {full_name}")
        try:  # pylint: disable=too-many-try-statements
            raw_table = self._external_catalog.getTable(database, table)
            table_format = self._option_as_python(raw_table.provider()) or "UNKNOWN"
            location_uri = self._option_as_python(raw_table.storage().locationUri())
            if location_uri:
                location_uri = location_uri.toString()
            is_partitioned = raw_table.partitionColumnNames().iterator().hasNext()
            object_type = raw_table.tableType().name()
            view_text = self._option_as_python(raw_table.viewText())
            table_properties = list(self._iterator(raw_table.properties()))
            formatted_table_properties = self._format_properties_list(table_properties)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"failed-table-crawl: describing table -> {full_name}: {e}", exc_info=True)
            return None
        return Table(
            catalog=catalog,
            database=database,
            name=table,
            object_type=object_type,
            table_format=table_format,
            location=location_uri,
            view_text=view_text,
            storage_properties=formatted_table_properties,
            is_partitioned=is_partitioned,
        )

    def _crawl(self) -> Iterable[Table]:
        """Crawls and lists tables within the specified catalog and database."""
        tasks = []
        catalog_tables: Collection[Table]
        catalog = "hive_metastore"
        databases = self._all_databases()
        for database in databases:
            logger.info(f"Scanning {database}")
            table_names = self._get_table_names(database)
            tasks.extend(self._create_describe_tasks(catalog, database, table_names))
        catalog_tables, errors = Threads.gather("describing tables in ", tasks)
        if len(errors) > 0:
            logger.warning(f"Detected {len(errors)} errors while scanning tables in ")

        logger.info(f"Finished scanning {len(catalog_tables)} tables")
        return catalog_tables

    def _get_table_names(self, database: str) -> list[str]:
        """
        Lists tables names in the specified database.
        :param database:
        :return: list of table names
        """
        table_names = []
        table_names_batches = Threads.strict('listing tables', [partial(self._list_tables, database)])
        for table_batch in table_names_batches:
            table_names.extend(table_batch)
        return table_names

    def _create_describe_tasks(self, catalog: str, database: str, table_names: list[str]) -> list[partial]:
        """
        Creates a list of partial functions for describing tables.
        :param catalog:
        :param database:
        :param table_names:
        :return: list of partial functions
        """
        tasks = []
        for table in table_names:
            tasks.append(partial(self._describe, catalog, database, table))
        return tasks
