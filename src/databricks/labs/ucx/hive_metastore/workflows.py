from databricks.labs.ucx.assessment.workflows import Assessment
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task
from databricks.labs.ucx.hive_metastore.tables import What


class TableMigration(Workflow):
    def __init__(self):
        super().__init__('migrate-tables')

    @job_task(job_cluster="main", depends_on=[Assessment.crawl_tables])
    def convert_managed_table(self, ctx: RuntimeContext):
        """This workflow task converts managed HMS tables to external table if `managed_table_external_storage` is set to `CONVERT_TO_EXTERNAL
        See documentation for more detail."""
        ctx.tables_migrator.convert_managed_hms_to_external(
            managed_table_external_storage=ctx.config.managed_table_external_storage
        )

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables, convert_managed_table])
    def migrate_external_tables_sync(self, ctx: RuntimeContext):
        """This workflow task migrates the external tables that are supported by SYNC command from the Hive Metastore
        to the Unity Catalog.
        """
        ctx.tables_migrator.migrate_tables(
            what=What.EXTERNAL_SYNC, managed_table_external_storage=ctx.config.managed_table_external_storage
        )

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables, convert_managed_table])
    def migrate_dbfs_root_delta_tables(self, ctx: RuntimeContext):
        """This workflow task migrates delta tables stored in DBFS root from the Hive Metastore to the Unity Catalog
        using deep clone.
        """
        ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA)

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables, convert_managed_table])
    def migrate_dbfs_root_non_delta_tables(
        self,
        ctx: RuntimeContext,
    ):
        """This workflow task migrates non delta tables stored in DBFS root from the Hive Metastore to the Unity Catalog
        using CTAS.
        """
        ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_NON_DELTA)

    @job_task(
        job_cluster="table_migration",
        depends_on=[
            Assessment.crawl_tables,
            migrate_external_tables_sync,
            migrate_dbfs_root_delta_tables,
            migrate_dbfs_root_non_delta_tables,
        ],
    )
    def migrate_views(self, ctx: RuntimeContext):
        """This workflow task migrates views from the Hive Metastore to the Unity Catalog using create view sql
        statement. It is dependent on the migration of the tables.
        """
        ctx.tables_migrator.migrate_tables(what=What.VIEW)

    @job_task(job_cluster="tacl")
    def setup_tacl(self, ctx: RuntimeContext):
        """(Optimization) Allow the TACL job cluster to be started before we commence refreshing the tables inventory."""

    @job_task(
        depends_on=[
            convert_managed_table,
            migrate_external_tables_sync,
            migrate_dbfs_root_delta_tables,
            migrate_dbfs_root_non_delta_tables,
            migrate_views,
            setup_tacl,
        ],
        job_cluster="tacl",
    )
    def update_table_inventory(self, ctx: RuntimeContext) -> None:
        """Refresh the tables inventory, prior to updating the migration status of all the tables."""
        # The TACL cluster is not UC-enabled, so we cannot the snapshot cannot be written immediately to the history log.
        # Step 1 of 3: Just refresh the tables inventory.
        ctx.tables_crawler.snapshot(force_refresh=True)

    @job_task(depends_on=[update_table_inventory], job_cluster="table_migration")
    def update_migration_status(self, ctx: RuntimeContext) -> None:
        """Scan the tables (and views) in the inventory and record whether each has been migrated or not."""
        # Step 2 of 3: Refresh the migration status of all the tables (updated in the previous step on a TACL cluster.)
        updated_migration_progress = ctx.migration_status_refresher.snapshot(force_refresh=True)
        ctx.tables_migrator.check_remaining_tables(updated_migration_progress)

    @job_task(depends_on=[update_migration_status], job_cluster="table_migration")
    def update_tables_history_log(self, ctx: RuntimeContext) -> None:
        """Update the history log with the latest tables inventory and migration status."""
        # The table migration cluster is not legacy-ACL enabled, so we can't crawl from here.
        # Step 3 of 3: Assuming (due to depends-on) the inventory and migration status was refreshed, capture into the
        # history log.
        # WARNING: this will fail if the tables inventory is empty, because it will then try to perform a crawl.
        history_log = ctx.tables_progress
        tables_snapshot = ctx.tables_crawler.snapshot()
        # Note: encoding the Table records will trigger loading of the migration-status data.
        history_log.append_inventory_snapshot(tables_snapshot)

    @job_task(job_cluster="table_migration", depends_on=[update_tables_history_log])
    def record_workflow_run(self, ctx: RuntimeContext) -> None:
        """Record the workflow run of this workflow."""
        ctx.workflow_run_recorder.record()


class MigrateHiveSerdeTablesInPlace(Workflow):
    def __init__(self):
        super().__init__('migrate-external-hiveserde-tables-in-place-experimental')

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables])
    def migrate_hive_serde_in_place(self, ctx: RuntimeContext):
        """This workflow task migrates ParquetHiveSerDe, OrcSerde, AvroSerDe tables in place from
        the Hive Metastore to the Unity Catalog."""
        ctx.tables_migrator.migrate_tables(
            what=What.EXTERNAL_HIVESERDE,
            hiveserde_in_place_migrate=True,
        )

    @job_task(
        job_cluster="table_migration",
        depends_on=[Assessment.crawl_tables, migrate_hive_serde_in_place],
    )
    def migrate_views(self, ctx: RuntimeContext):
        """This workflow task migrates views from the Hive Metastore to the Unity Catalog using create view sql statement.
        It is dependent on the migration of the tables.
        """
        ctx.tables_migrator.migrate_tables(what=What.VIEW)

    @job_task(job_cluster="tacl")
    def setup_tacl(self, ctx: RuntimeContext):
        """(Optimization) Allow the TACL job cluster to be started before we commence refreshing the tables inventory."""

    @job_task(depends_on=[migrate_hive_serde_in_place, migrate_views, setup_tacl], job_cluster="tacl")
    def update_table_inventory(self, ctx: RuntimeContext) -> None:
        """Refresh the tables inventory, prior to updating the migration status of all the tables."""
        # The TACL cluster is not UC-enabled, so we cannot the snapshot cannot be written immediately to the history log.
        # Step 1 of 3: Just refresh the tables inventory.
        ctx.tables_crawler.snapshot(force_refresh=True)

    @job_task(depends_on=[update_table_inventory], job_cluster="table_migration")
    def update_migration_status(self, ctx: RuntimeContext) -> None:
        """Scan the tables (and views) in the inventory and record whether each has been migrated or not."""
        # Step 2 of 3: Refresh the migration status of all the tables (updated in the previous step on a TACL cluster.)
        updated_migration_progress = ctx.migration_status_refresher.snapshot(force_refresh=True)
        ctx.tables_migrator.check_remaining_tables(updated_migration_progress)

    @job_task(depends_on=[update_migration_status], job_cluster="table_migration")
    def update_tables_history_log(self, ctx: RuntimeContext) -> None:
        """Update the history log with the latest tables inventory and migration status."""
        # The table migration cluster is not legacy-ACL enabled, so we can't crawl from here.
        # Step 3 of 3: Assuming (due to depends-on) the inventory and migration status was refreshed, capture into the
        # history log.
        # WARNING: this will fail if the tables inventory is empty, because it will then try to perform a crawl.
        history_log = ctx.tables_progress
        tables_snapshot = ctx.tables_crawler.snapshot()
        # Note: encoding the Table records will trigger loading of the migration-status data.
        history_log.append_inventory_snapshot(tables_snapshot)

    @job_task(job_cluster="table_migration", depends_on=[update_tables_history_log])
    def record_workflow_run(self, ctx: RuntimeContext) -> None:
        """Record the workflow run of this workflow."""
        ctx.workflow_run_recorder.record()


class MigrateExternalTablesCTAS(Workflow):
    def __init__(self):
        super().__init__('migrate-external-tables-ctas')

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables])
    def migrate_other_external_ctas(self, ctx: RuntimeContext):
        """This workflow task migrates non-SYNC supported and non HiveSerde external tables using CTAS"""
        ctx.tables_migrator.migrate_tables(
            what=What.EXTERNAL_NO_SYNC,
        )

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables])
    def migrate_hive_serde_ctas(self, ctx: RuntimeContext):
        """This workflow task migrates HiveSerde tables using CTAS"""
        ctx.tables_migrator.migrate_tables(
            what=What.EXTERNAL_HIVESERDE,
        )

    @job_task(
        job_cluster="table_migration",
        depends_on=[Assessment.crawl_tables, migrate_other_external_ctas, migrate_hive_serde_ctas],
    )
    def migrate_views(self, ctx: RuntimeContext):
        """This workflow task migrates views from the Hive Metastore to the Unity Catalog using create view sql
        statement. It is dependent on the migration of the tables.
        """
        ctx.tables_migrator.migrate_tables(what=What.VIEW)

    @job_task(job_cluster="tacl")
    def setup_tacl(self, ctx: RuntimeContext):
        """(Optimization) Allow the TACL job cluster to be started before we commence refreshing the tables inventory."""

    @job_task(
        depends_on=[migrate_other_external_ctas, migrate_hive_serde_ctas, migrate_views, setup_tacl], job_cluster="tacl"
    )
    def update_table_inventory(self, ctx: RuntimeContext) -> None:
        """Refresh the tables inventory, prior to updating the migration status of all the tables."""
        # The TACL cluster is not UC-enabled, so we cannot the snapshot cannot be written immediately to the history log.
        # Step 1 of 3: Just refresh the tables inventory.
        ctx.tables_crawler.snapshot(force_refresh=True)

    @job_task(depends_on=[update_table_inventory], job_cluster="table_migration")
    def update_migration_status(self, ctx: RuntimeContext) -> None:
        """Scan the tables (and views) in the inventory and record whether each has been migrated or not."""
        # Step 2 of 3: Refresh the migration status of all the tables (updated in the previous step on a TACL cluster.)
        updated_migration_progress = ctx.migration_status_refresher.snapshot(force_refresh=True)
        ctx.tables_migrator.check_remaining_tables(updated_migration_progress)

    @job_task(depends_on=[update_migration_status], job_cluster="table_migration")
    def update_tables_history_log(self, ctx: RuntimeContext) -> None:
        """Update the history log with the latest tables inventory and migration status."""
        # The table migration cluster is not legacy-ACL enabled, so we can't crawl from here.
        # Step 3 of 3: Assuming (due to depends-on) the inventory and migration status was refreshed, capture into the
        # history log.
        # WARNING: this will fail if the tables inventory is empty, because it will then try to perform a crawl.
        history_log = ctx.tables_progress
        tables_snapshot = ctx.tables_crawler.snapshot()
        # Note: encoding the Table records will trigger loading of the migration-status data.
        history_log.append_inventory_snapshot(tables_snapshot)

    @job_task(job_cluster="table_migration", depends_on=[update_tables_history_log])
    def record_workflow_run(self, ctx: RuntimeContext) -> None:
        """Record the workflow run of this workflow."""
        ctx.workflow_run_recorder.record()


class ScanTablesInMounts(Workflow):
    def __init__(self):
        super().__init__('scan-tables-in-mounts-experimental')

    @job_task
    def scan_tables_in_mounts_experimental(self, ctx: RuntimeContext):
        """[EXPERIMENTAL] This workflow scans for Delta tables inside all mount points
        captured during the assessment. It will store the results in the `tables` table,
        replacing any existing content that might be present."""
        ctx.tables_in_mounts.snapshot(force_refresh=True)

    @job_task(depends_on=[scan_tables_in_mounts_experimental], job_cluster="table_migration")
    def update_migration_status(self, ctx: RuntimeContext) -> None:
        """Scan the tables (and views) in the inventory and record whether each has been migrated or not."""
        updated_migration_progress = ctx.migration_status_refresher.snapshot(force_refresh=True)
        ctx.tables_migrator.check_remaining_tables(updated_migration_progress)

    @job_task(depends_on=[update_migration_status], job_cluster="table_migration")
    def update_tables_history_log(self, ctx: RuntimeContext) -> None:
        """Update the history log with the latest tables inventory and migration status."""
        # WARNING: this will fail if the tables inventory is empty, because it will then try to perform a crawl.
        history_log = ctx.tables_progress
        tables_snapshot = ctx.tables_crawler.snapshot()
        # Note: encoding the Table records will trigger loading of the migration-status data.
        history_log.append_inventory_snapshot(tables_snapshot)

    @job_task(job_cluster="table_migration", depends_on=[update_tables_history_log])
    def record_workflow_run(self, ctx: RuntimeContext) -> None:
        """Record the workflow run of this workflow."""
        ctx.workflow_run_recorder.record()


class MigrateTablesInMounts(Workflow):
    def __init__(self):
        super().__init__('migrate-tables-in-mounts-experimental')

    @job_task(job_cluster="table_migration", depends_on=[ScanTablesInMounts.scan_tables_in_mounts_experimental])
    def migrate_tables_in_mounts_experimental(self, ctx: RuntimeContext):
        """[EXPERIMENTAL] This workflow migrates `delta tables stored in mount points` to Unity Catalog using a Create Table statement."""
        ctx.tables_migrator.migrate_tables(what=What.TABLE_IN_MOUNT)

    @job_task(job_cluster="tacl")
    def setup_tacl(self, ctx: RuntimeContext):
        """(Optimization) Allow the TACL job cluster to be started before we commence refreshing the tables inventory."""

    @job_task(depends_on=[migrate_tables_in_mounts_experimental, setup_tacl], job_cluster="tacl")
    def update_table_inventory(self, ctx: RuntimeContext) -> None:
        """Refresh the tables inventory, prior to updating the migration status of all the tables."""
        # The TACL cluster is not UC-enabled, so we cannot the snapshot cannot be written immediately to the history log.
        # Step 1 of 3: Just refresh the tables inventory.
        ctx.tables_crawler.snapshot(force_refresh=True)

    @job_task(depends_on=[update_table_inventory], job_cluster="table_migration")
    def update_migration_status(self, ctx: RuntimeContext) -> None:
        """Scan the tables (and views) in the inventory and record whether each has been migrated or not."""
        # Step 2 of 3: Refresh the migration status of all the tables (updated in the previous step on a TACL cluster.)
        updated_migration_progress = ctx.migration_status_refresher.snapshot(force_refresh=True)
        ctx.tables_migrator.check_remaining_tables(updated_migration_progress)

    @job_task(depends_on=[update_migration_status], job_cluster="table_migration")
    def update_tables_history_log(self, ctx: RuntimeContext) -> None:
        """Update the history log with the latest tables inventory and migration status."""
        # The table migration cluster is not legacy-ACL enabled, so we can't crawl from here.
        # Step 3 of 3: Assuming (due to depends-on) the inventory and migration status was refreshed, capture into the
        # history log.
        # WARNING: this will fail if the tables inventory is empty, because it will then try to perform a crawl.
        history_log = ctx.tables_progress
        tables_snapshot = ctx.tables_crawler.snapshot()
        # Note: encoding the Table records will trigger loading of the migration-status data.
        history_log.append_inventory_snapshot(tables_snapshot)

    @job_task(job_cluster="table_migration", depends_on=[update_tables_history_log])
    def record_workflow_run(self, ctx: RuntimeContext) -> None:
        """Record the workflow run of this workflow."""
        ctx.workflow_run_recorder.record()
