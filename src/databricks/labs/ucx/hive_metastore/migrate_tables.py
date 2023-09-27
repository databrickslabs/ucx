import logging
from functools import partial

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.framework.parallel import ThreadedExecution
from databricks.labs.ucx.hive_metastore.tables import TablesCrawler

logger = logging.getLogger(__name__)


class TablesMigrate:
    def __init__(
        self,
        tc: TablesCrawler,
        ws: WorkspaceClient,
        backend: SqlBackend,
        default_catalog=None,
        inventory_database="",
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
            return "ucx_default"  # TODO : Fetch current workspace id

    def migrate_tables(self):
        tasks = []
        if self._database_to_catalog_mapping:
            for table in self._tc.snapshot():
                tasks.append(partial(self._migrate_table, self._database_to_catalog_mapping[table.database], table))
            ThreadedExecution.gather("migrate tables", tasks)
        else:
            for table in self._tc.snapshot():
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
