import dataclasses
import logging
from collections import defaultdict
from collections.abc import Iterable
from functools import partial

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.hive_metastore import TablesCrawler, Mounts
from databricks.labs.ucx.hive_metastore.grants import Grant, GrantsCrawler, PrincipalACL
from databricks.labs.ucx.hive_metastore.locations import Mount, ExternalLocations
from databricks.labs.ucx.hive_metastore.mapping import (
    Rule,
    TableMapping,
    TableToMigrate,
)
from databricks.labs.ucx.hive_metastore.migration_status import MigrationStatusRefresher
from databricks.labs.ucx.hive_metastore.tables import (
    AclMigrationWhat,
    MigrationCount,
    Table,
    What,
    HiveSerdeType,
)
from databricks.labs.ucx.hive_metastore.view_migrate import (
    ViewsMigrationSequencer,
    ViewToMigrate,
)
from databricks.labs.ucx.workspace_access.groups import GroupManager, MigratedGroup

logger = logging.getLogger(__name__)


class TablesMigrator:
    def __init__(
        self,
        table_crawler: TablesCrawler,
        grant_crawler: GrantsCrawler,
        ws: WorkspaceClient,
        backend: SqlBackend,
        table_mapping: TableMapping,
        group_manager: GroupManager,
        migration_status_refresher: 'MigrationStatusRefresher',
        principal_grants: PrincipalACL,
    ):
        self._tc = table_crawler
        self._gc = grant_crawler
        self._backend = backend
        self._ws = ws
        self._tm = table_mapping
        self._group = group_manager
        self._migration_status_refresher = migration_status_refresher
        self._seen_tables: dict[str, str] = {}
        self._principal_grants = principal_grants

    def index(self):
        # TODO: remove this method
        return self._migration_status_refresher.index()

    def migrate_tables(
        self,
        what: What,
        acl_strategy: list[AclMigrationWhat] | None = None,
        mounts_crawler: Mounts | None = None,
        hiveserde_in_place_migrate: bool = False,
    ):
        if what in [What.DB_DATASET, What.UNKNOWN]:
            logger.error(f"Can't migrate tables with type {what.name}")
            return None
        all_grants_to_migrate = None if acl_strategy is None else self._gc.snapshot()
        all_migrated_groups = None if acl_strategy is None else self._group.snapshot()
        all_principal_grants = None if acl_strategy is None else self._principal_grants.get_interactive_cluster_grants()
        self._init_seen_tables()
        # mounts will be used to replace the mnt based table location in the DDL for hiveserde table in-place migration
        mounts: list[Mount] = []
        if mounts_crawler:
            mounts = list(mounts_crawler.snapshot())
        if what == What.VIEW:
            return self._migrate_views(acl_strategy, all_grants_to_migrate, all_migrated_groups, all_principal_grants)
        return self._migrate_tables(
            what,
            acl_strategy,
            all_grants_to_migrate,
            all_migrated_groups,
            all_principal_grants,
            mounts,
            hiveserde_in_place_migrate,
        )

    def _migrate_tables(
        self,
        what: What,
        acl_strategy,
        all_grants_to_migrate,
        all_migrated_groups,
        all_principal_grants,
        mounts: list[Mount],
        hiveserde_in_place_migrate: bool = False,
    ):
        tables_to_migrate = self._tm.get_tables_to_migrate(self._tc)
        tables_in_scope = filter(lambda t: t.src.what == what, tables_to_migrate)
        tasks = []
        for table in tables_in_scope:
            grants = self._compute_grants(
                table.src, acl_strategy, all_grants_to_migrate, all_migrated_groups, all_principal_grants
            )
            tasks.append(partial(self._migrate_table, table, grants, mounts, hiveserde_in_place_migrate))
        Threads.strict("migrate tables", tasks)
        if not tasks:
            logger.info(f"No tables found to migrate with type {what.name}")
        # the below is useful for testing
        return tasks

    def _migrate_views(self, acl_strategy, all_grants_to_migrate, all_migrated_groups, all_principal_grants):
        tables_to_migrate = self._tm.get_tables_to_migrate(self._tc)
        self._migration_status_refresher.reset()
        all_tasks = []
        sequencer = ViewsMigrationSequencer(tables_to_migrate, self.index())
        batches = sequencer.sequence_batches()
        for batch in batches:
            tasks = []
            for view in batch:
                grants = self._compute_grants(
                    view.src, acl_strategy, all_grants_to_migrate, all_migrated_groups, all_principal_grants
                )
                tasks.append(
                    partial(
                        self._migrate_view,
                        view,
                        grants,
                    )
                )
            Threads.strict("migrate views", tasks)
            self._migration_status_refresher.reset()
            all_tasks.extend(tasks)
        return all_tasks

    def _compute_grants(
        self, table: Table, acl_strategy, all_grants_to_migrate, all_migrated_groups, all_principal_grants
    ):
        if acl_strategy is None:
            acl_strategy = []
        grants = []
        if AclMigrationWhat.LEGACY_TACL in acl_strategy:
            grants.extend(self._match_grants(table, all_grants_to_migrate, all_migrated_groups))
        if AclMigrationWhat.PRINCIPAL in acl_strategy:
            grants.extend(self._match_grants(table, all_principal_grants, all_migrated_groups))
        return grants

    def _migrate_table(
        self,
        src_table: TableToMigrate,
        grants: list[Grant],
        mounts: list[Mount],
        hiveserde_in_place_migrate: bool = False,
    ):
        if self._table_already_migrated(src_table.rule.as_uc_table_key):
            logger.info(f"Table {src_table.src.key} already migrated to {src_table.rule.as_uc_table_key}")
            return True
        if src_table.src.what == What.DBFS_ROOT_DELTA:
            return self._migrate_dbfs_root_table(src_table.src, src_table.rule, grants)
        if src_table.src.what == What.DBFS_ROOT_NON_DELTA:
            return self._migrate_table_create_ctas(src_table.src, src_table.rule, grants, mounts)
        if src_table.src.what == What.EXTERNAL_SYNC:
            return self._migrate_external_table(src_table.src, src_table.rule, grants)
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
                return self._migrate_external_table_hiveserde_in_place(src_table.src, src_table.rule, grants, mounts)
            return self._migrate_table_create_ctas(src_table.src, src_table.rule, grants, mounts)
        if src_table.src.what == What.EXTERNAL_NO_SYNC:
            # use CTAS if table cannot be upgraded using SYNC and table is not hiveserde table
            return self._migrate_table_create_ctas(src_table.src, src_table.rule, grants, mounts)
        logger.info(f"Table {src_table.src.key} is not supported for migration")
        return True

    def _migrate_view(
        self,
        src_view: ViewToMigrate,
        grants: list[Grant] | None = None,
    ):
        if self._table_already_migrated(src_view.rule.as_uc_table_key):
            logger.info(f"View {src_view.src.key} already migrated to {src_view.rule.as_uc_table_key}")
            return True
        if self._view_can_be_migrated(src_view):
            return self._migrate_view_table(src_view, grants)
        logger.info(f"View {src_view.src.key} is not supported for migration")
        return True

    def _view_can_be_migrated(self, view: ViewToMigrate):
        # dependencies have already been computed, therefore an empty dict is good enough
        for table in view.dependencies:
            if not self.index().get(table.schema, table.name):
                logger.info(f"View {view.src.key} cannot be migrated because {table.key} is not migrated yet")
                return False
        return True

    def _migrate_view_table(self, src_view: ViewToMigrate, grants: list[Grant] | None = None):
        view_migrate_sql = self._sql_migrate_view(src_view)
        logger.debug(f"Migrating view {src_view.src.key} to using SQL query: {view_migrate_sql}")
        self._backend.execute(view_migrate_sql)
        self._backend.execute(src_view.src.sql_alter_to(src_view.rule.as_uc_table_key))
        self._backend.execute(src_view.src.sql_alter_from(src_view.rule.as_uc_table_key, self._ws.get_workspace_id()))
        return self._migrate_acl(src_view.src, src_view.rule, grants)

    def _sql_migrate_view(self, src_view: ViewToMigrate) -> str:
        # We have to fetch create statement this way because of columns in:
        # CREATE VIEW x.y (col1, col2) AS SELECT * FROM w.t
        create_statement = self._backend.fetch(f"SHOW CREATE TABLE {src_view.src.safe_sql_key}")
        src_view.src.view_text = next(iter(create_statement))["createtab_stmt"]
        migration_index = self._migration_status_refresher.index()
        return src_view.sql_migrate_view(migration_index)

    def _migrate_external_table(self, src_table: Table, rule: Rule, grants: list[Grant] | None = None):
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.sql_migrate_external(target_table_key)
        logger.debug(f"Migrating external table {src_table.key} to using SQL query: {table_migrate_sql}")
        # have to wrap the fetch result with iter() for now, because StatementExecutionBackend returns iterator but RuntimeBackend returns list.
        sync_result = next(iter(self._backend.fetch(table_migrate_sql)))
        if sync_result.status_code != "SUCCESS":
            logger.warning(
                f"SYNC command failed to migrate {src_table.key} to {target_table_key}. Status code: {sync_result.status_code}. Description: {sync_result.description}"
            )
            return False
        self._backend.execute(src_table.sql_alter_from(rule.as_uc_table_key, self._ws.get_workspace_id()))
        return self._migrate_acl(src_table, rule, grants)

    def _migrate_external_table_hiveserde_in_place(
        self,
        src_table: Table,
        rule: Rule,
        grants: list[Grant],
        mounts: list[Mount],
    ):
        # verify hive serde type
        hiveserde_type = src_table.hiveserde_type(self._backend)
        if hiveserde_type in [
            HiveSerdeType.NOT_HIVESERDE,
            HiveSerdeType.OTHER_HIVESERDE,
            HiveSerdeType.INVALID_HIVESERDE_INFO,
        ]:
            logger.warning(f"{src_table.key} table can only be migrated using CTAS.")
            return False

        # if the src table location is using mount, resolve the mount location so it will be used in the updated DDL
        dst_table_location = None
        if mounts and src_table.is_dbfs_mnt:
            dst_table_location = ExternalLocations.resolve_mount(src_table.location, mounts)

        table_migrate_sql = src_table.sql_migrate_external_hiveserde_in_place(
            rule.catalog_name, rule.dst_schema, rule.dst_table, self._backend, hiveserde_type, dst_table_location
        )
        if not table_migrate_sql:
            logger.error(
                f"Failed to generate in-place migration DDL for {src_table.key}, skip the in-place migration. It can be migrated in CTAS workflow"
            )
            return False

        logger.debug(
            f"Migrating external table {src_table.key} to {rule.as_uc_table_key} using SQL query: {table_migrate_sql}"
        )
        self._backend.execute(table_migrate_sql)
        self._backend.execute(src_table.sql_alter_to(rule.as_uc_table_key))
        self._backend.execute(src_table.sql_alter_from(rule.as_uc_table_key, self._ws.get_workspace_id()))
        return self._migrate_acl(src_table, rule, grants)

    def _migrate_dbfs_root_table(self, src_table: Table, rule: Rule, grants: list[Grant] | None = None):
        target_table_key = rule.as_uc_table_key
        table_migrate_sql = src_table.sql_migrate_dbfs(target_table_key)
        logger.debug(
            f"Migrating managed table {src_table.key} to {rule.as_uc_table_key} using SQL query: {table_migrate_sql}"
        )
        self._backend.execute(table_migrate_sql)
        self._backend.execute(src_table.sql_alter_to(rule.as_uc_table_key))
        self._backend.execute(src_table.sql_alter_from(rule.as_uc_table_key, self._ws.get_workspace_id()))
        return self._migrate_acl(src_table, rule, grants)

    def _migrate_table_create_ctas(self, src_table: Table, rule: Rule, grants: list[Grant], mounts: list[Mount]):
        if src_table.what not in [What.EXTERNAL_NO_SYNC, What.EXTERNAL_HIVESERDE]:
            table_migrate_sql = src_table.sql_migrate_ctas_managed(rule.as_uc_table_key)
        elif not src_table.location:
            table_migrate_sql = src_table.sql_migrate_ctas_managed(rule.as_uc_table_key)
        else:
            # if external table and src tabel location is not missing, migrate to external UC table
            dst_table_location = src_table.location + "_ctas_migrated"
            if mounts and src_table.is_dbfs_mnt:
                dst_table_location = ExternalLocations.resolve_mount(src_table.location, mounts) + "_ctas_migrated"
            table_migrate_sql = src_table.sql_migrate_ctas_external(rule.as_uc_table_key, dst_table_location)
        logger.debug(f"Migrating table {src_table.key} to {rule.as_uc_table_key} using SQL query: {table_migrate_sql}")
        self._backend.execute(table_migrate_sql)
        self._backend.execute(src_table.sql_alter_to(rule.as_uc_table_key))
        self._backend.execute(src_table.sql_alter_from(rule.as_uc_table_key, self._ws.get_workspace_id()))
        return self._migrate_acl(src_table, rule, grants)

    def _migrate_acl(self, src: Table, rule: Rule, grants: list[Grant] | None):
        if grants is None:
            return True
        for grant in grants:
            acl_migrate_sql = grant.uc_grant_sql(src.kind, rule.as_uc_table_key)
            if acl_migrate_sql is None:
                logger.warning(f"Cannot identify UC grant for {src.kind} {rule.as_uc_table_key}. Skipping.")
                continue
            logger.debug(f"Migrating acls on {rule.as_uc_table_key} using SQL query: {acl_migrate_sql}")
            self._backend.execute(acl_migrate_sql)
        return True

    def _table_already_migrated(self, target) -> bool:
        return target in self._seen_tables

    def _get_tables_to_revert(self, schema: str | None = None, table: str | None = None) -> list[Table]:
        schema = schema.lower() if schema else None
        table = table.lower() if table else None
        migrated_tables = []
        if table and not schema:
            logger.error("Cannot accept 'Table' parameter without 'Schema' parameter")

        for cur_table in self._tc.snapshot():
            if schema and cur_table.database != schema:
                continue
            if table and cur_table.name != table:
                continue
            if cur_table.key in self._seen_tables.values():
                migrated_tables.append(cur_table)
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

    def _revert_migrated_table(self, table: Table, target_table_key: str):
        logger.info(
            f"Reverting {table.object_type} table {table.database}.{table.name} upgraded_to {table.upgraded_to}"
        )
        self._backend.execute(table.sql_unset_upgraded_to())
        self._backend.execute(f"DROP {table.kind} IF EXISTS {target_table_key}")

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

    def print_revert_report(self, *, delete_managed: bool) -> bool | None:
        migrated_count = self._get_revert_count()
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

    def _init_seen_tables(self):
        self._seen_tables = self._migration_status_refresher.get_seen_tables()

    @staticmethod
    def _match_grants(table: Table, grants: Iterable[Grant], migrated_groups: list[MigratedGroup]) -> list[Grant]:
        matched_grants = []
        for grant in grants:
            if grant.database != table.database:
                continue
            if table.name not in (grant.table, grant.view):
                continue
            matched_group = [g.name_in_account for g in migrated_groups if g.name_in_workspace == grant.principal]
            if len(matched_group) > 0:
                grant = dataclasses.replace(grant, principal=matched_group[0])
            matched_grants.append(grant)
        return matched_grants
