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
