import dataclasses
import logging
import re
from collections import defaultdict
from collections.abc import Iterable
from functools import partial, cached_property

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import DatabricksError

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import MigrateGrants
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations
from databricks.labs.ucx.hive_metastore.mapping import (
    Rule,
    TableMapping,
    TableToMigrate,
)
from databricks.labs.ucx.hive_metastore.table_migration_status import (
    TableMigrationStatusRefresher,
    TableMigrationStatus,
    TableMigrationIndex,
)
from databricks.labs.ucx.hive_metastore.tables import (
    MigrationCount,
    Table,
    What,
    HiveSerdeType,
)
from databricks.labs.ucx.hive_metastore.view_migrate import (
    ViewsMigrationSequencer,
    ViewToMigrate,
)


logger = logging.getLogger(__name__)


class TablesMigrator:
    def __init__(
        self,
        tables_crawler: TablesCrawler,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        table_mapping: TableMapping,
        migration_status_refresher: TableMigrationStatusRefresher,
        migrate_grants: MigrateGrants,
        external_locations: ExternalLocations,
    ):

        self._tables_crawler = tables_crawler
        self._sql_backend = sql_backend
        self._ws = ws
        self._table_mapping = table_mapping
        self._migration_status_refresher = migration_status_refresher
        self._seen_tables: dict[str, str] = {}
        self._migrate_grants = migrate_grants
        self._external_locations = external_locations

    def warn_about_remaining_non_migrated_tables(self, migration_statuses: Iterable[TableMigrationStatus]) -> None:
        migration_index = TableMigrationIndex(migration_statuses)
        for crawled_table in self._tables_crawler.snapshot():
            if not migration_index.is_migrated(crawled_table.database, crawled_table.name):
                logger.warning(f"remained-hive-metastore-table: {crawled_table.key}")

    def index(self, *, force_refresh: bool = False):
        return self._migration_status_refresher.index(force_refresh=force_refresh)

    def convert_managed_hms_to_external(
        self,
        managed_table_external_storage: str = "CLONE",
    ):
        # This method contains some of the steps of migrate tables. this was done to separate out the
        # code for converting managed hms table to external, since this needs to run in non uc cluster,
        # the functionality to call the UC api are removed

        if managed_table_external_storage != "CONVERT_TO_EXTERNAL":
            logger.info("Not required to convert managed hms table to external, Skipping this task...")
            return None
        self._spark = self._spark_session
        tables_to_migrate = self._table_mapping.get_tables_to_migrate(self._tables_crawler, False)
        tables_in_scope = filter(lambda t: t.src.what == What.EXTERNAL_SYNC, tables_to_migrate)
        tasks = []
        for table in tables_in_scope:
            tasks.append(
                partial(
                    self._convert_hms_table_to_external,
                    table.src,
                )
            )
        Threads.strict("convert tables", tasks)
        if not tasks:
            logger.info("No managed hms table found to convert to external")
        return tasks

    def convert_wasbs_to_adls_gen2(self):
        """
        Converts a Hive metastore azure wasbs tables to abfss using spark jvm.
        """

        self._spark = self._spark_session
        tables_to_migrate = self._table_mapping.get_tables_to_migrate(self._tables_crawler, False)
        tables_in_scope = filter(lambda t: t.src.what == What.EXTERNAL_SYNC, tables_to_migrate)
        tasks = []
        for table in tables_in_scope:
            if table.src.location and table.src.location.startswith("wasbs://"):
                tasks.append(
                    partial(
                        self._convert_wasbs_table_to_abfss,
                        table.src,
                    )
                )
        Threads.strict("convert tables to abfss", tasks)
        if not tasks:
            logger.info("No wasbs table found to convert to abfss")
        return tasks

    def migrate_tables(
        self,
        what: What,
        hiveserde_in_place_migrate: bool = False,
        managed_table_external_storage: str = "CLONE",
        check_uc_table: bool = True,
    ):
        if managed_table_external_storage == "CONVERT_TO_EXTERNAL":
            self._spark = self._spark_session
        if what in [What.DB_DATASET, What.UNKNOWN]:
            logger.error(f"Can't migrate tables with type {what.name}")
            return None
        self._init_seen_tables()
        if what == What.VIEW:
            return self._migrate_views()
        return self._migrate_tables(
            what, managed_table_external_storage.upper(), hiveserde_in_place_migrate, check_uc_table
        )

    def _migrate_tables(
        self,
        what: What,
        managed_table_external_storage: str,
        hiveserde_in_place_migrate: bool = False,
        check_uc_table: bool = True,
    ):
        tables_to_migrate = self._table_mapping.get_tables_to_migrate(self._tables_crawler, check_uc_table)
        tables_in_scope = filter(lambda t: t.src.what == what, tables_to_migrate)
        tasks = []
        for table in tables_in_scope:
            tasks.append(
                partial(
                    self._safe_migrate_table,
                    table,
                    managed_table_external_storage,
                    hiveserde_in_place_migrate,
                )
            )
        Threads.strict("migrate tables", tasks)
        if not tasks:
            logger.info(f"No tables found to migrate with type {what.name}")
        # the below is useful for testing
        return tasks

    def _migrate_views(self):
        tables_to_migrate = self._table_mapping.get_tables_to_migrate(self._tables_crawler)
        all_tasks = []
        # Every batch of views to migrate needs an up-to-date table migration index
        # to determine if the dependencies have been migrated
        migration_index = self.index(force_refresh=True)
        sequencer = ViewsMigrationSequencer(tables_to_migrate, migration_index=migration_index)
        batches = sequencer.sequence_batches()
        for batch in batches:
            # Avoid redundant refresh for first batch.
            if all_tasks:
                migration_index = self.index(force_refresh=True)
            tasks = [partial(self._migrate_view, view, migration_index) for view in batch]
            Threads.strict("migrate views", tasks)
            all_tasks.extend(tasks)
        return all_tasks

    @cached_property
    def _spark_session(self):
        # pylint: disable-next=import-error,import-outside-toplevel
        from pyspark.sql.session import SparkSession  # type: ignore[import-not-found]

        return SparkSession.builder.getOrCreate()

    def _migrate_managed_table(
        self,
        managed_table_external_storage: str,
        src_table: TableToMigrate,
    ):
        if managed_table_external_storage == 'CONVERT_TO_EXTERNAL':
            return self._migrate_external_table(
                src_table.src, src_table.rule
            )  # _migrate_external_table remains unchanged
        if managed_table_external_storage == 'SYNC_AS_EXTERNAL':
            return self._migrate_managed_as_external_table(src_table.src, src_table.rule)  # new method
        if managed_table_external_storage == 'CLONE':
            return self._migrate_table_create_ctas(src_table.src, src_table.rule)
        logger.warning(f"failed-to-migrate: unknown managed_table_external_storage: {managed_table_external_storage}")
        return False

    def _safe_migrate_table(
        self,
        src_table: TableToMigrate,
        managed_table_external_storage: str,
        hiveserde_in_place_migrate: bool = False,
    ) -> bool:
        if self._table_already_migrated(src_table.rule.as_uc_table_key):
            logger.info(f"Table {src_table.src.key} already migrated to {src_table.rule.as_uc_table_key}")
            return True
        try:
            return self._migrate_table(src_table, managed_table_external_storage, hiveserde_in_place_migrate)
        except Exception as e:  # pylint: disable=broad-exception-caught
            # Catching a Spark AnalysisException here, for which we do not have the dependency to catch explicitly
            pattern = (  # See https://github.com/databrickslabs/ucx/issues/2891
                r"INVALID_PARAMETER_VALUE: Invalid input: RPC CreateTable Field managedcatalog.ColumnInfo.name: "
                r'At columns.\d+: name "" is not a valid name`'
            )
            if re.match(pattern, str(e)):
                logger.warning(f"failed-to-migrate: Table with empty column name '{src_table.src.key}'", exc_info=e)
            else:
                logger.warning(f"failed-to-migrate: Unknown reason for table '{src_table.src.key}'", exc_info=e)
            return False

    def _migrate_table(
        self,
        src_table: TableToMigrate,
        managed_table_external_storage: str,
        hiveserde_in_place_migrate: bool = False,
    ) -> bool:
        if src_table.src.what == What.DBFS_ROOT_DELTA:
            return self._migrate_dbfs_root_table(src_table.src, src_table.rule)
        if src_table.src.what == What.DBFS_ROOT_NON_DELTA:
            return self._migrate_table_create_ctas(src_table.src, src_table.rule)
        if src_table.src.is_managed:
            return self._migrate_managed_table(managed_table_external_storage, src_table)
        if src_table.src.what == What.EXTERNAL_SYNC:
            return self._migrate_external_table(src_table.src, src_table.rule)
        if src_table.src.what == What.TABLE_IN_MOUNT:
            return self._migrate_table_in_mount(src_table.src, src_table.rule)
        if src_table.src.what == What.EXTERNAL_HIVESERDE:
            # This hiveserde_in_place_migrate is used to determine if current hiveserde migration should use in-place migration or CTAS.
            # We will provide two workflows for hiveserde table migration:
            # 1. One will migrate all hiveserde tables using CTAS which we officially support.
            # 2. The other one will migrate certain types of hiveserde in place, which is technically working, but the user
            # need to accept the risk that the old files created by hiveserde may not be processed correctly by Spark
            # datasource in corner cases.
            # User will need to decide which workflow to runs first which will migrate the hiveserde tables and mark the
            # `upgraded_to` property and hence those tables will be skipped in the migration workflow runs later.
            if hiveserde_in_place_migrate:
                return self._migrate_external_table_hiveserde_in_place(src_table.src, src_table.rule)
            return self._migrate_table_create_ctas(src_table.src, src_table.rule)
        if src_table.src.what == What.EXTERNAL_NO_SYNC:
            # use CTAS if table cannot be upgraded using SYNC and table is not hiveserde table
            return self._migrate_table_create_ctas(src_table.src, src_table.rule)
        logger.info(f"Table {src_table.src.key} is not supported for migration")
        return True

    def _migrate_view(self, src_view: ViewToMigrate, migration_index: TableMigrationIndex) -> bool:
        if self._table_already_migrated(src_view.rule.as_uc_table_key):
            logger.info(f"View {src_view.src.key} already migrated to {src_view.rule.as_uc_table_key}")
            return True
        if self._view_can_be_migrated(src_view, migration_index):
            return self._migrate_view_table(src_view, migration_index)
        logger.info(f"View {src_view.src.key} is not supported for migration")
        return True

    def _view_can_be_migrated(self, view: ViewToMigrate, migration_index: TableMigrationIndex) -> bool:
        # dependencies have already been computed, therefore an empty dict is good enough
        for table in view.dependencies:
            if not migration_index.get(table.schema, table.name):
                logger.info(f"View {view.src.key} cannot be migrated because {table.key} is not migrated yet")
                return False
        return True

    def _migrate_view_table(self, src_view: ViewToMigrate, migration_index: TableMigrationIndex):
        view_migrate_sql = self._sql_migrate_view(src_view, migration_index)
        logger.debug(f"Migrating view {src_view.src.key} to using SQL query: {view_migrate_sql}")
        try:
            self._sql_backend.execute(view_migrate_sql)
            self._sql_backend.execute(self._sql_alter_to(src_view.src, src_view.rule.as_uc_table_key))
            self._sql_backend.execute(
                self._sql_alter_from(src_view.src, src_view.rule.as_uc_table_key, self._ws.get_workspace_id())
            )
        except DatabricksError as e:
            logger.warning(f"Failed to migrate view {src_view.src.key} to {src_view.rule.as_uc_table_key}: {e}")
            return False
        return self._migrate_grants.apply(src_view.src, src_view.rule.as_uc_table)

    def _sql_migrate_view(self, src_view: ViewToMigrate, migration_index: TableMigrationIndex) -> str:
        # We have to fetch create statement this way because of columns in:
        # CREATE VIEW x.y (col1, col2) AS SELECT * FROM w.t
        create_statement = self._sql_backend.fetch(f"SHOW CREATE TABLE {src_view.src.safe_sql_key}")
        src_view.src.view_text = next(iter(create_statement))["createtab_stmt"]
        # this does not require the index to be refreshed because the dependencies have already been validated
        return src_view.sql_migrate_view(migration_index)

    @cached_property
    def _catalog(self):
        return self._spark._jsparkSession.sessionState().catalog()  # pylint: disable=protected-access

    @cached_property
    def _table_identifier(self):
        return self._spark._jvm.org.apache.spark.sql.catalyst.TableIdentifier  # pylint: disable=protected-access

    @cached_property
    def _catalog_type(self):
        return (
            self._spark._jvm.org.apache.spark.sql.catalyst.catalog.CatalogTableType  # pylint: disable=protected-access
        )

    @cached_property
    def _catalog_table(self):
        return self._spark._jvm.org.apache.spark.sql.catalyst.catalog.CatalogTable  # pylint: disable=protected-access

    @cached_property
    def _catalog_storage(self):
        return (
            self._spark._jvm.org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat  # pylint: disable=protected-access
        )

    @staticmethod
    def _get_entity_storage_locations(table_metadata):
        """Obtain the entityStorageLocations property for table metadata, if the property is present."""
        # This is needed because:
        #  - DBR 16.0 introduced entityStorageLocations as a property on table metadata, and this is required for
        #    as a constructor parameter for CatalogTable.
        #  - We need to be compatible with earlier versions of DBR.
        #  - The normal hasattr() check does not work with Py4J-based objects: it always returns True and non-existent
        #    methods will be automatically created on the proxy but fail when invoked.
        # Instead the only approach is to use dir() to check if the method exists _prior_ to trying to access it.
        # (After trying to access it, dir() will also include it even though it doesn't exist.)
        return table_metadata.entityStorageLocations() if 'entityStorageLocations' in dir(table_metadata) else None

    def _convert_hms_table_to_external(self, src_table: Table) -> bool:
        """Converts a Hive metastore table to external using Spark JVM methods."""
        logger.info(f"Changing HMS managed table {src_table.name} to External Table type.")
        inventory_table = self._tables_crawler.full_name
        try:
            database = self._spark._jvm.scala.Some(src_table.database)  # pylint: disable=protected-access
            table_identifier = self._table_identifier(src_table.name, database)
            old_table = self._catalog.getTableMetadata(table_identifier)
            entity_storage_locations = self._get_entity_storage_locations(old_table)
            new_table = self._catalog_table(
                old_table.identifier(),
                self._catalog_type('EXTERNAL'),
                old_table.storage(),
                old_table.schema(),
                old_table.provider(),
                old_table.partitionColumnNames(),
                old_table.bucketSpec(),
                old_table.owner(),
                old_table.createTime(),
                old_table.lastAccessTime(),
                old_table.createVersion(),
                old_table.properties(),
                old_table.stats(),
                old_table.viewText(),
                old_table.comment(),
                old_table.unsupportedFeatures(),
                old_table.tracksPartitionsInCatalog(),
                old_table.schemaPreservesCase(),
                old_table.ignoredProperties(),
                old_table.viewOriginalText(),
                # From DBR 16, there's a new constructor argument: entityStorageLocations (Seq[EntityStorageLocation])
                # (We can't detect whether the argument is needed by the constructor, but assume that if the accessor
                # is present on the source table then the argument is needed.)
                *([entity_storage_locations] if entity_storage_locations is not None else []),
            )
            self._catalog.alterTable(new_table)
            self._update_table_status(src_table, inventory_table)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Error converting HMS table {src_table.name} to external: {e}", exc_info=True)
            return False
        logger.info(f"Converted {src_table.name} to External Table type.")
        return True

    def _convert_wasbs_table_to_abfss(self, src_table: Table) -> bool:
        """
        Converts a Hive metastore azure wasbs table to abfss using alter table command.
        """
        logger.info(f"Changing HMS managed table {src_table.name} to External Table type.")
        inventory_table = self._tables_crawler.full_name
        database = self._spark._jvm.scala.Some(src_table.database)  # pylint: disable=protected-access
        table_identifier = self._table_identifier(src_table.name, database)
        new_table_location = ExternalLocations.wasbs_to_abfss(src_table.location)
        if not new_table_location:
            logger.warning(f"Invalid wasbs location for table {src_table.name}, skipping conversion.")
            return False
        try:
            old_table = self._catalog.getTableMetadata(table_identifier)
            entity_storage_locations = self._get_entity_storage_locations(old_table)
            table_location = old_table.storage()
            new_location = self._catalog_storage(
                self._spark._jvm.scala.Some(  # pylint: disable=protected-access
                    self._spark._jvm.java.net.URI(new_table_location)  # pylint: disable=protected-access
                ),
                table_location.inputFormat(),
                table_location.outputFormat(),
                table_location.serde(),
                table_location.compressed(),
                table_location.properties(),
            )
            new_table = self._catalog_table(
                old_table.identifier(),
                old_table.tableType(),
                new_location,
                old_table.schema(),
                old_table.provider(),
                old_table.partitionColumnNames(),
                old_table.bucketSpec(),
                old_table.owner(),
                old_table.createTime(),
                old_table.lastAccessTime(),
                old_table.createVersion(),
                old_table.properties(),
                old_table.stats(),
                old_table.viewText(),
                old_table.comment(),
                old_table.unsupportedFeatures(),
                old_table.tracksPartitionsInCatalog(),
                old_table.schemaPreservesCase(),
                old_table.ignoredProperties(),
                old_table.viewOriginalText(),
                # From DBR 16, there's a new constructor argument: entityStorageLocations (Seq[EntityStorageLocation])
                # (We can't detect whether the argument is needed by the constructor, but assume that if the accessor
                # is present on the source table then the argument is needed.)
                *([entity_storage_locations] if entity_storage_locations is not None else []),
            )
            self._catalog.alterTable(new_table)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Error converting HMS table {src_table.name} to abfss: {e}", exc_info=True)
            return False
        self._update_table_location(src_table, inventory_table, new_table_location)
        logger.info(f"Converted {src_table.name} to External Table type.")
        return True

    def _update_table_status(self, src_table: Table, inventory_table: str) -> None:
        update_sql = f"UPDATE {escape_sql_identifier(inventory_table)} SET object_type = 'EXTERNAL' WHERE catalog='hive_metastore' AND database='{src_table.database}' AND name='{src_table.name}';"
        self._sql_backend.execute(update_sql)

    def _update_table_location(self, src_table: Table, inventory_table: str, new_location: str) -> None:
        update_sql = f"UPDATE {escape_sql_identifier(inventory_table)} SET location = '{new_location}' WHERE catalog='hive_metastore' AND database='{src_table.database}' AND name='{src_table.name}';"
        self._sql_backend.execute(update_sql)

    def _migrate_managed_as_external_table(self, src_table: Table, rule: Rule) -> bool:
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.sql_migrate_as_external(target_table_key)
        logger.debug(f"Migrating external table {src_table.key} to using SQL query: {table_migrate_sql}")
        # have to wrap the fetch result with iter() for now, because StatementExecutionBackend returns iterator but RuntimeBackend returns list.
        sync_result = next(iter(self._sql_backend.fetch(table_migrate_sql)))
        if sync_result.status_code != "SUCCESS":
            logger.warning(
                f"failed-to-migrate: SYNC command failed to migrate table {src_table.key} to {target_table_key}. "
                f"Status code: {sync_result.status_code}. Description: {sync_result.description}"
            )
            return False
        self._sql_backend.execute(self._sql_alter_from(src_table, rule.as_uc_table_key, self._ws.get_workspace_id()))
        return self._migrate_grants.apply(src_table, rule.as_uc_table)

    def _migrate_external_table(self, src_table: Table, rule: Rule) -> bool:
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.sql_migrate_external(target_table_key)
        logger.debug(f"Migrating external table {src_table.key} to using SQL query: {table_migrate_sql}")
        # have to wrap the fetch result with iter() for now, because StatementExecutionBackend returns iterator but RuntimeBackend returns list.
        sync_result = next(iter(self._sql_backend.fetch(table_migrate_sql)))
        if sync_result.status_code != "SUCCESS":
            logger.warning(
                f"failed-to-migrate: SYNC command failed to migrate table {src_table.key} to {target_table_key}. "
                f"Status code: {sync_result.status_code}. Description: {sync_result.description}"
            )
            return False
        self._sql_backend.execute(self._sql_alter_from(src_table, rule.as_uc_table_key, self._ws.get_workspace_id()))
        return self._migrate_grants.apply(src_table, rule.as_uc_table)

    def _migrate_external_table_hiveserde_in_place(self, src_table: Table, rule: Rule) -> bool:
        # verify hive serde type
        hiveserde_type = src_table.hiveserde_type(self._sql_backend)
        if hiveserde_type in [
            HiveSerdeType.NOT_HIVESERDE,
            HiveSerdeType.OTHER_HIVESERDE,
            HiveSerdeType.INVALID_HIVESERDE_INFO,
        ]:
            logger.warning(f"{src_table.key} table can only be migrated using CTAS.")
            return False

        # if the src table location is using mount, resolve the mount location so it will be used in the updated DDL
        dst_table_location = None
        if src_table.is_dbfs_mnt:
            dst_table_location = self._external_locations.resolve_mount(src_table.location)

        table_migrate_sql = src_table.sql_migrate_external_hiveserde_in_place(
            rule.catalog_name, rule.dst_schema, rule.dst_table, self._sql_backend, hiveserde_type, dst_table_location
        )
        if not table_migrate_sql:
            logger.error(
                f"Failed to generate in-place migration DDL for {src_table.key}, skip the in-place migration. It can be migrated in CTAS workflow"
            )
            return False

        logger.debug(
            f"Migrating external table {src_table.key} to {rule.as_uc_table_key} using SQL query: {table_migrate_sql}"
        )
        try:
            self._sql_backend.execute(table_migrate_sql)
            self._sql_backend.execute(self._sql_alter_to(src_table, rule.as_uc_table_key))
            self._sql_backend.execute(self._sql_add_migrated_comment(src_table, rule.as_uc_table_key))
            self._sql_backend.execute(
                self._sql_alter_from(src_table, rule.as_uc_table_key, self._ws.get_workspace_id())
            )
        except DatabricksError as e:
            logger.warning(f"failed-to-migrate: Failed to migrate table {src_table.key} to {rule.as_uc_table_key}: {e}")
            return False
        return self._migrate_grants.apply(src_table, rule.as_uc_table)

    def _migrate_dbfs_root_table(self, src_table: Table, rule: Rule) -> bool:
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.sql_migrate_dbfs(target_table_key)
        logger.debug(
            f"Migrating managed table {src_table.key} to {rule.as_uc_table_key} using SQL query: {table_migrate_sql}"
        )
        try:
            self._sql_backend.execute(table_migrate_sql)
            self._sql_backend.execute(self._sql_alter_to(src_table, rule.as_uc_table_key))
            self._sql_backend.execute(self._sql_add_migrated_comment(src_table, rule.as_uc_table_key))
            self._sql_backend.execute(
                self._sql_alter_from(src_table, rule.as_uc_table_key, self._ws.get_workspace_id())
            )
        except DatabricksError as e:
            logger.warning(f"failed-to-migrate: Failed to migrate table {src_table.key} to {rule.as_uc_table_key}: {e}")
            return False
        return self._migrate_grants.apply(src_table, rule.as_uc_table)

    def _migrate_table_create_ctas(self, src_table: Table, rule: Rule) -> bool:
        if src_table.what not in [What.EXTERNAL_NO_SYNC, What.EXTERNAL_HIVESERDE]:
            table_migrate_sql = src_table.sql_migrate_ctas_managed(rule.as_uc_table_key)
        elif not src_table.location:
            table_migrate_sql = src_table.sql_migrate_ctas_managed(rule.as_uc_table_key)
        else:
            # if external table and src tabel location is not missing, migrate to external UC table
            dst_table_location = src_table.location + "_ctas_migrated"
            if src_table.is_dbfs_mnt:
                resolved_mount = self._external_locations.resolve_mount(src_table.location)
                if resolved_mount:
                    dst_table_location = resolved_mount + "_ctas_migrated"
            table_migrate_sql = src_table.sql_migrate_ctas_external(rule.as_uc_table_key, dst_table_location)
        logger.debug(f"Migrating table {src_table.key} to {rule.as_uc_table_key} using SQL query: {table_migrate_sql}")
        try:
            self._sql_backend.execute(table_migrate_sql)
            self._sql_backend.execute(self._sql_alter_to(src_table, rule.as_uc_table_key))
            self._sql_backend.execute(self._sql_add_migrated_comment(src_table, rule.as_uc_table_key))
            self._sql_backend.execute(
                self._sql_alter_from(src_table, rule.as_uc_table_key, self._ws.get_workspace_id())
            )
        except DatabricksError as e:
            logger.warning(f"failed-to-migrate: Failed to migrate table {src_table.key} to {rule.as_uc_table_key}: {e}")
            return False
        return self._migrate_grants.apply(src_table, rule.as_uc_table)

    def _migrate_table_in_mount(self, src_table: Table, rule: Rule) -> bool:
        target_table_key = rule.as_uc_table_key
        try:
            table_schema = self._sql_backend.fetch(f"DESCRIBE TABLE delta.`{src_table.location}`;")
            table_migrate_sql = src_table.sql_migrate_table_in_mount(target_table_key, table_schema)
            logger.info(
                f"Migrating table in mount {src_table.location} to UC table {rule.as_uc_table_key} using SQL query: {table_migrate_sql}"
            )
            self._sql_backend.execute(table_migrate_sql)
            self._sql_backend.execute(
                self._sql_alter_from(src_table, rule.as_uc_table_key, self._ws.get_workspace_id())
            )
        except DatabricksError as e:
            logger.warning(f"failed-to-migrate: Failed to migrate table {src_table.key} to {rule.as_uc_table_key}: {e}")
            return False
        return self._migrate_grants.apply(src_table, rule.as_uc_table)

    def _table_already_migrated(self, target) -> bool:
        return target in self._seen_tables

    def _get_tables_to_revert(self, schema: str | None = None, table: str | None = None) -> list[Table]:
        schema = schema.lower() if schema else None
        table = table.lower() if table else None
        reverse_seen = {v: k for (k, v) in self._seen_tables.items()}
        migrated_tables = []
        if table and not schema:
            logger.error("Cannot accept 'Table' parameter without 'Schema' parameter")
        for cur_table in self._tables_crawler.snapshot():
            if schema and cur_table.database != schema:
                continue
            if table and cur_table.name != table:
                continue
            if cur_table.key in reverse_seen:
                updated_table = dataclasses.replace(cur_table, upgraded_to=reverse_seen[cur_table.key])
                migrated_tables.append(updated_table)
        return migrated_tables

    def revert_migrated_tables(
        self, schema: str | None = None, table: str | None = None, *, delete_managed: bool = False
    ):
        self._init_seen_tables()
        migrated_tables = self._get_tables_to_revert(schema=schema, table=table)
        # reverses the _seen_tables dictionary to key by the source table
        reverse_seen = {v: k for (k, v) in self._seen_tables.items()}
        tasks = []
        for migrated_table in migrated_tables:
            if migrated_table.kind == "VIEW" or migrated_table.object_type == "EXTERNAL" or delete_managed:
                tasks.append(partial(self._revert_migrated_table, migrated_table, reverse_seen[migrated_table.key]))
                continue
            logger.info(
                f"Skipping {migrated_table.object_type} Table {migrated_table.database}.{migrated_table.name} "
                f"upgraded_to {migrated_table.upgraded_to}"
            )
        Threads.strict("revert migrated tables", tasks)

    def _revert_migrated_table(self, table: Table, target_table_key: str) -> None:
        logger.info(
            f"Reverting {table.object_type} table {table.database}.{table.name} upgraded_to {table.upgraded_to}"
        )
        try:
            self._sql_backend.execute(table.sql_unset_upgraded_to())
            self._sql_backend.execute(f"DROP {table.kind} IF EXISTS {escape_sql_identifier(target_table_key)}")
        except DatabricksError as e:
            logger.warning(f"Failed to revert table {table.key}: {e}")

    def _get_revert_count(self, schema: str | None = None, table: str | None = None) -> list[MigrationCount]:
        self._init_seen_tables()
        migrated_tables = self._get_tables_to_revert(schema=schema, table=table)
        table_by_database = defaultdict(list)
        for cur_table in migrated_tables:
            table_by_database[cur_table.database].append(cur_table)

        migration_list = []
        for cur_database, tables in table_by_database.items():
            what_count: dict[What, int] = {}
            for current_table in tables:
                if current_table.upgraded_to is not None:
                    count = what_count.get(current_table.what, 0)
                    what_count[current_table.what] = count + 1
            migration_list.append(MigrationCount(database=cur_database, what_count=what_count))
        return migration_list

    def is_migrated(self, schema: str, table: str) -> bool:
        return self._migration_status_refresher.is_migrated(schema, table)

    def print_revert_report(
        self,
        *,
        schema: str | None = None,
        table: str | None = None,
        delete_managed: bool,
    ) -> bool | None:
        migrated_count = self._get_revert_count(schema=schema, table=table)
        if not migrated_count:
            logger.info("No migrated tables were found.")
            return False
        print("The following is the count of migrated tables and views found in scope:")
        table_header = "Database            |"
        headers = 1
        for what in list(What):
            headers = max(headers, len(what.name.split("_")))
            table_header += f" {what.name.split('_')[0]:<10} |"
        print(table_header)
        # Split the header so _ separated what names are splitted into multiple lines
        for header in range(1, headers):
            table_sub_header = "                    |"
            for what in list(What):
                if len(what.name.split("_")) - 1 < header:
                    table_sub_header += f"{' ' * 12}|"
                    continue
                table_sub_header += f" {what.name.split('_')[header]:<10} |"
            print(table_sub_header)
        separator = "=" * (22 + 13 * len(What))
        print(separator)
        for count in migrated_count:
            table_row = f"{count.database:<20}|"
            for what in list(What):
                table_row += f" {count.what_count.get(what, 0):10} |"
            print(table_row)
        print(separator)
        print("The following actions will be performed")
        print("- Migrated External Tables and Views (targets) will be deleted")
        if delete_managed:
            print("- Migrated DBFS Root Tables will be deleted")
        else:
            print("- Migrated DBFS Root Tables will be left intact")
            print("To revert and delete Migrated Tables, add --delete_managed true flag to the command")
        return True

    def _init_seen_tables(self) -> None:
        self._seen_tables = self._migration_status_refresher.get_seen_tables()

    def _sql_alter_to(self, table: Table, target_table_key: str) -> str:
        return f"ALTER {table.kind} {escape_sql_identifier(table.key)} SET TBLPROPERTIES ('upgraded_to' = '{target_table_key}');"

    def _sql_add_migrated_comment(self, table: Table, target_table_key: str) -> str:
        """Docs: https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html#add-comments-to-indicate-that-a-hive-table-has-been-migrated"""
        return f"COMMENT ON {table.kind} {escape_sql_identifier(table.key)} IS 'This {table.kind.lower()} is deprecated. Please use `{target_table_key}` instead of `{table.key}`.';"

    def _sql_alter_from(self, table: Table, target_table_key: str, ws_id: int) -> str:
        """Adds a property to the table indicating the source of the migration.
        This is used to track the source of the migration for auditing purposes.
        Args:
            table: The table being migrated.
            target_table_key: The key of the target table.
            ws_id: The workspace ID where the migration is happening.
        Returns:
            str: The SQL command to alter the table and set the properties.
        """

        source = table.location if table.is_table_in_mount else table.key
        return (
            f"ALTER {table.kind} {escape_sql_identifier(target_table_key)} SET TBLPROPERTIES "
            f"('upgraded_from' = '{source}'"
            f" , '{table.UPGRADED_FROM_WS_PARAM}' = '{ws_id}');"
        )
