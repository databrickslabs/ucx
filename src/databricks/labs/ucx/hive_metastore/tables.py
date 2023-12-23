import logging
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import partial

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.framework.parallel import Threads
from databricks.labs.ucx.mixins.sql import Row

logger = logging.getLogger(__name__)


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

    def sql_unset_to(self, catalog):
        return f"ALTER {self.kind} {catalog}.{self.database}.{self.name} UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"

    # SQL to reset the assessment record to revert migration state
    def sql_unset_to_assessment(self, schema):
        return f"UPDATE {schema}.tables SET upgraded_to=NULL where DATABASE = {self.database} and NAME = {self.name}"


@dataclass
class TableError:
    catalog: str
    database: str
    name: str | None = None
    error: str | None = None


class TablesCrawler(CrawlerBase):
    def __init__(self, backend: SqlBackend, schema):
        """
        Initializes a TablesCrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "tables", Table)

    def _all_databases(self) -> Iterator[Row]:
        yield from self._fetch("SHOW DATABASES")

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

    def _try_load(self) -> Iterable[Table]:
        """Tries to load table information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {self._full_name}"):
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
        for (database,) in self._all_databases():
            logger.debug(f"[{catalog}.{database}] listing tables")
            for _, table, _is_tmp in self._fetch(f"SHOW TABLES FROM {catalog}.{database}"):
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
            for key, value, _ in self._fetch(f"DESCRIBE TABLE EXTENDED {full_name}"):
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
        except Exception as e:
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            logger.error(f"Couldn't fetch information for table {full_name} : {e}")
            return None

    def unset_upgraded_to(self, database: str | None = None, name: str | None = None):
        filter_exp = " "
        if database and name:
            filter_exp = f" WHERE database='{database}' AND name='{name}'"
        elif database:
            filter_exp = f" WHERE database='{database}'"
        self._backend.execute(f"UPDATE {self._full_name} SET upgraded_to=NULL{filter_exp}")


class TablesMigrate:
    def __init__(
        self,
        tc: TablesCrawler,
        ws: WorkspaceClient,
        backend: SqlBackend,
        default_catalog=None,
        database_to_catalog_mapping: dict[str, str] | None = None,
    ):
        self._tc = tc
        self._backend = backend
        self._ws = ws
        self._database_to_catalog_mapping = database_to_catalog_mapping
        self._default_catalog = self._init_default_catalog(default_catalog)
        self._seen_tables: dict[str, str] = {}

    @staticmethod
    def _init_default_catalog(default_catalog):
        if default_catalog:
            return default_catalog
        else:
            return "ucx_default"  # TODO : Fetch current workspace name and append it to the default catalog.

    def migrate_tables(self):
        self._init_seen_tables()
        tasks = []
        for table in self._tc.snapshot():
            target_catalog = self._default_catalog
            if self._database_to_catalog_mapping:
                target_catalog = self._database_to_catalog_mapping[table.database]
            tasks.append(partial(self._migrate_table, target_catalog, table))
        _, errors = Threads.gather("migrate tables", tasks)
        if len(errors) > 0:
            # TODO: https://github.com/databrickslabs/ucx/issues/406
            # TODO: pick first X issues in the summary
            msg = f"Detected {len(errors)} errors: {'. '.join(str(e) for e in errors)}"
            raise ValueError(msg)

    def _migrate_table(self, target_catalog: str, table: Table):
        sql = table.uc_create_sql(target_catalog)
        logger.debug(f"Migrating table {table.key} to using SQL query: {sql}")
        target = f"{target_catalog}.{table.database}.{table.name}".lower()

        if self._table_already_upgraded(target):
            logger.info(f"Table {table.key} already upgraded to {self._seen_tables[target]}")
        elif table.object_type == "MANAGED":
            self._backend.execute(sql)
            self._backend.execute(table.sql_alter_to(target_catalog))
            self._backend.execute(table.sql_alter_from(target_catalog))
            self._seen_tables[target] = table.key
        elif table.object_type == "EXTERNAL":
            result = next(self._backend.fetch(sql))
            if result.status_code != "SUCCESS":
                raise ValueError(result.description)
            self._backend.execute(table.sql_alter_to(target_catalog))
            self._backend.execute(table.sql_alter_from(target_catalog))
            self._seen_tables[target] = table.key
        else:
            msg = f"Table {table.key} is a {table.object_type} and is not supported for migration yet"
            raise ValueError(msg)
        return True

    def _init_seen_tables(self):
        for catalog in self._ws.catalogs.list():
            for schema in self._ws.schemas.list(catalog_name=catalog.name):
                for table in self._ws.tables.list(catalog_name=catalog.name, schema_name=schema.name):
                    if table.properties is not None and "upgraded_from" in table.properties:
                        self._seen_tables[table.full_name.lower()] = table.properties["upgraded_from"].lower()

    def _table_already_upgraded(self, target) -> bool:
        return target in self._seen_tables

    def revert_migrated_tables(self, *, schema: str | None = None, table: str | None = None):
        def scope_filter(cur_table: Table):
            schema_match = not schema or cur_table.database == schema
            # if schema is specified matches the schema
            table_match = not table or cur_table.name == table
            # if table is specified matches the table
            return schema_match and table_match

        upgraded_tables = []
        for cur_table in list(self._tc.snapshot()):
            if scope_filter(cur_table) and cur_table.upgraded_to is not None:
                upgraded_tables.append(cur_table)

        for upgraded_table in upgraded_tables:
            logger.info(
                f"Reverting Table {upgraded_table.database}.{upgraded_table.name} "
                f"upgraded_to {upgraded_table.upgraded_to}"
            )
            self._backend.execute(upgraded_table.sql_unset_to("hive_metastore"))
        self._tc.unset_upgraded_to(database=schema, name=table)
