import csv
import dataclasses
import io
import logging
import re
from dataclasses import dataclass
from functools import partial

from databricks.labs.blueprint.parallel import Threads
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, NotFound, ResourceConflict
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.account import WorkspaceInfo
from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import Table

logger = logging.getLogger(__name__)


@dataclass
class Rule:
    workspace_name: str
    catalog_name: str
    src_schema: str
    dst_schema: str
    src_table: str
    dst_table: str

    @classmethod
    def initial(cls, workspace_name: str, catalog_name: str, table: Table) -> "Rule":
        return cls(
            workspace_name=workspace_name,
            catalog_name=catalog_name,
            src_schema=table.database,
            dst_schema=table.database,
            src_table=table.name,
            dst_table=table.name,
        )

    @property
    def as_uc_table_key(self):
        return f"{self.catalog_name}.{self.dst_schema}.{self.dst_table}"

    @property
    def as_hms_table_key(self):
        return f"hive_metastore.{self.src_schema}.{self.src_table}"


@dataclass
class TableToMigrate:
    src: Table
    rule: Rule


class TableMapping:
    UCX_SKIP_PROPERTY = "databricks.labs.ucx.skip"

    def __init__(self, ws: WorkspaceClient, backend: SqlBackend, folder: str | None = None):
        if not folder:
            folder = f"/Users/{ws.current_user.me().user_name}/.ucx"
        self._ws = ws
        self._folder = folder
        self._backend = backend
        self._field_names = [_.name for _ in dataclasses.fields(Rule)]

    def current_tables(self, tables: TablesCrawler, workspace_name: str, catalog_name: str):
        tables_snapshot = tables.snapshot()
        if len(tables_snapshot) == 0:
            msg = "No tables found. Please run: databricks labs ucx ensure-assessment-run"
            raise ValueError(msg)
        for table in tables_snapshot:
            yield Rule.initial(workspace_name, catalog_name, table)

    def save(self, tables: TablesCrawler, workspace_info: WorkspaceInfo) -> str:
        workspace_name = workspace_info.current()
        default_catalog_name = re.sub(r"\W+", "_", workspace_name)
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, self._field_names)
        writer.writeheader()
        for rule in self.current_tables(tables, workspace_name, default_catalog_name):
            writer.writerow(dataclasses.asdict(rule))
        buffer.seek(0)
        return self._overwrite_mapping(buffer)

    def load(self) -> list[Rule]:
        try:
            rules = []
            remote = self._ws.workspace.download(f"{self._folder}/mapping.csv")
            for row in csv.DictReader(remote):  # type: ignore[arg-type]
                rules.append(Rule(**row))
            return rules
        except NotFound:
            msg = "Please run: databricks labs ucx table-mapping"
            raise ValueError(msg) from None

    def skip_table(self, schema: str, table: str):
        # Marks a table to be skipped in the migration process by applying a table property
        try:
            self._backend.execute(
                f"ALTER TABLE `{schema}`.`{table}` SET TBLPROPERTIES('{self.UCX_SKIP_PROPERTY}' = true)"
            )
        except NotFound as nf:
            if "[TABLE_OR_VIEW_NOT_FOUND]" in str(nf) or "[DELTA_TABLE_NOT_FOUND]" in str(nf):
                logger.error(f"Failed to apply skip marker for Table {schema}.{table}. Table not found.")
            else:
                logger.error(f"Failed to apply skip marker for Table {schema}.{table}: {nf!s}", exc_info=True)
        except BadRequest as br:
            logger.error(f"Failed to apply skip marker for Table {schema}.{table}: {br!s}", exc_info=True)

    def skip_schema(self, schema: str):
        # Marks a schema to be skipped in the migration process by applying a table property
        try:
            self._backend.execute(f"ALTER SCHEMA `{schema}` SET DBPROPERTIES('{self.UCX_SKIP_PROPERTY}' = true)")
        except NotFound as nf:
            if "[SCHEMA_NOT_FOUND]" in str(nf):
                logger.error(f"Failed to apply skip marker for Schema {schema}. Schema not found.")
            else:
                logger.error(nf)
        except BadRequest as br:
            logger.error(br)

    def get_tables_to_migrate(self, tables_crawler: TablesCrawler):
        rules = self.load()
        # Getting all the source tables from the rules
        databases_in_scope = self._get_databases_in_scope({rule.src_schema for rule in rules})
        crawled_tables_keys = {crawled_table.key: crawled_table for crawled_table in tables_crawler.snapshot()}
        tasks = []
        for rule in rules:
            if rule.as_hms_table_key not in crawled_tables_keys:
                logger.info(f"Table {rule.as_hms_table_key} in the mapping doesn't show up in assessment")
                continue
            if rule.src_schema not in databases_in_scope:
                logger.info(f"Table {rule.as_hms_table_key} is in a database that was marked to be skipped")
                continue
            if crawled_tables_keys[rule.as_hms_table_key].is_databricks_dataset:
                logger.info(f"Table {rule.as_hms_table_key} is a db demo dataset and will not be upgraded")
                continue
            tasks.append(
                partial(self._get_table_in_scope_task, TableToMigrate(crawled_tables_keys[rule.as_hms_table_key], rule))
            )

        return Threads.strict("checking all database properties", tasks)

    def _overwrite_mapping(self, buffer) -> str:
        path = f"{self._folder}/mapping.csv"
        self._ws.workspace.upload(path, buffer, overwrite=True, format=ImportFormat.AUTO)
        return path

    def _get_databases_in_scope(self, databases: set[str]):
        tasks = []
        for database in databases:
            tasks.append(partial(self._get_database_in_scope_task, database))
        return Threads.strict("checking databases for skip property", tasks)

    def _get_database_in_scope_task(self, database: str) -> str | None:
        describe = {}
        for value in self._backend.fetch(f"DESCRIBE SCHEMA EXTENDED {database}"):
            describe[value["database_description_item"]] = value["database_description_value"]
        if self.UCX_SKIP_PROPERTY in TablesCrawler.parse_database_props(describe.get("Properties", "").lower()):
            logger.info(f"Database {database} is marked to be skipped")
            return None
        return database

    def _get_table_in_scope_task(self, table_to_migrate: TableToMigrate) -> TableToMigrate | None:
        table = table_to_migrate.src
        rule = table_to_migrate.rule

        if self._exists_in_uc(table, rule.as_uc_table_key):
            logger.info(f"The intended target for {table.key}, {rule.as_uc_table_key}, already exists.")
            return None
        result = self._backend.fetch(f"SHOW TBLPROPERTIES `{table.database}`.`{table.name}`")
        for value in result:
            if value["key"] == self.UCX_SKIP_PROPERTY:
                logger.info(f"{table.key} is marked to be skipped")
                return None
            if value["key"] == "upgraded_to":
                logger.info(f"{table.key} is set as upgraded to {value['value']}")
                if self._exists_in_uc(table, value["value"]):
                    logger.info(
                        f"The table {table.key} was previously upgraded to {value['value']}. "
                        f"To revert the table and allow it to be upgraded again use the CLI command:"
                        f"databricks labs ucx revert --schema {table.database} --table {table.name}"
                    )
                    return None
                logger.info(f"The upgrade_to target for {table.key} is missing. Unsetting the upgrade_to property")
                self._backend.execute(table.sql_unset_upgraded_to())

        return table_to_migrate

    def _exists_in_uc(self, src_table: Table, target_key: str):
        # Attempts to get the target table info from UC returns True if it exists.
        try:
            table_info = self._ws.tables.get(target_key)
            if not table_info.properties:
                return True
            upgraded_from = table_info.properties.get("upgraded_from")
            if upgraded_from and upgraded_from != src_table.key:
                raise ResourceConflict(
                    f"Expected to be migrated from {src_table.key}, but got {upgraded_from}. "
                    "You can skip this error using the CLI command: "
                    "databricks labs ucx skip "
                    f"--schema {src_table.database} --table {src_table.name}"
                )
            return True
        except NotFound:
            return False
