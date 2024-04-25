from databricks.labs.ucx.assessment.workflows import Assessment
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task
from databricks.labs.ucx.hive_metastore.tables import AclMigrationWhat, What


class TableMigration(Workflow):
    def __init__(self):
        super().__init__('migrate-tables')

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables])
    def migrate_external_tables_sync(self, ctx: RuntimeContext):
        """This workflow task migrates the *external tables that are supported by SYNC command* from the Hive Metastore to the Unity Catalog.
        Following cli commands are required to be run before running this task:
        - For Azure: `principal-prefix-access`, `create-table-mapping`, `create-uber-principal`, `migrate-credentials`, `migrate-locations`, `create-catalogs-schemas`
        - For AWS: TBD
        """
        ctx.tables_migrator.migrate_tables(what=What.EXTERNAL_SYNC, acl_strategy=[AclMigrationWhat.LEGACY_TACL])

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables])
    def migrate_dbfs_root_delta_tables(self, ctx: RuntimeContext):
        """This workflow task migrates `delta tables stored in DBFS root` from the Hive Metastore to the Unity Catalog using deep clone.
        Following cli commands are required to be run before running this task:
        - For Azure: `principal-prefix-access`, `create-table-mapping`, `create-uber-principal`, `migrate-credentials`, `migrate-locations`, `create-catalogs-schemas`
        - For AWS: TBD
        """
        ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA, acl_strategy=[AclMigrationWhat.LEGACY_TACL])

    @job_task(
        job_cluster="table_migration",
        depends_on=[Assessment.crawl_tables, migrate_external_tables_sync, migrate_dbfs_root_delta_tables],
    )
    def migrate_views(self, ctx: RuntimeContext):
        """This workflow task migrates views from the Hive Metastore to the Unity Catalog using create view sql statement.
        It is dependent on the migration of the tables.
        """
        ctx.tables_migrator.migrate_tables(what=What.VIEW, acl_strategy=[AclMigrationWhat.LEGACY_TACL])


class MigrateHiveSerdeTablesInPlace(Workflow):
    def __init__(self):
        super().__init__('migrate-external-hiveserde-tables-in-place-experimental')

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables])
    def migrate_hive_serde_in_place(self, ctx: RuntimeContext):
        """This workflow task migrates ParquetHiveSerDe, OrcSerde, AvroSerDe tables in place from the Hive Metastore to the Unity Catalog."""
        ctx.tables_migrator.migrate_tables(
            what=What.EXTERNAL_HIVESERDE,
            acl_strategy=[AclMigrationWhat.LEGACY_TACL],
            mounts_crawler=ctx.mounts_crawler,
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
        ctx.tables_migrator.migrate_tables(what=What.VIEW, acl_strategy=[AclMigrationWhat.LEGACY_TACL])


class MigrateExternalTablesCTAS(Workflow):
    def __init__(self):
        super().__init__('migrate-external-tables-ctas')

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables])
    def migrate_other_external_ctas(self, ctx: RuntimeContext):
        """This workflow task migrates non SYNC supported and non HiveSerde external tables using CTAS"""
        ctx.tables_migrator.migrate_tables(
            what=What.EXTERNAL_NO_SYNC,
            acl_strategy=[AclMigrationWhat.LEGACY_TACL],
            mounts_crawler=ctx.mounts_crawler,
        )

    @job_task(job_cluster="table_migration", depends_on=[Assessment.crawl_tables])
    def migrate_hive_serde_ctas(self, ctx: RuntimeContext):
        """This workflow task migrates HiveSerde tables using CTAS"""
        ctx.tables_migrator.migrate_tables(
            what=What.EXTERNAL_HIVESERDE,
            acl_strategy=[AclMigrationWhat.LEGACY_TACL],
            mounts_crawler=ctx.mounts_crawler,
        )

    @job_task(
        job_cluster="table_migration",
        depends_on=[Assessment.crawl_tables, migrate_other_external_ctas, migrate_hive_serde_ctas],
    )
    def migrate_views(self, ctx: RuntimeContext):
        """This workflow task migrates views from the Hive Metastore to the Unity Catalog using create view sql statement.
        It is dependent on the migration of the tables.
        """
        ctx.tables_migrator.migrate_tables(what=What.VIEW, acl_strategy=[AclMigrationWhat.LEGACY_TACL])


class MigrateTablesInMounts(Workflow):
    def __init__(self):
        super().__init__('migrate-tables-in-mounts-experimental')

    @job_task
    def scan_tables_in_mounts_experimental(self, ctx: RuntimeContext):
        """[EXPERIMENTAL] This workflow scans for Delta tables inside all mount points
        captured during the assessment. It will store the results under the `tables` table
        located under the assessment."""
        ctx.tables_in_mounts.snapshot()
