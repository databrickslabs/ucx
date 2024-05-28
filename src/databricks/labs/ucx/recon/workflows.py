from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task


class MigrationRecon(Workflow):
    def __init__(self):
        super().__init__('migrate-data-reconciliation')

    @job_task(job_cluster="table_migration")
    def recon_migration_result(self, ctx: RuntimeContext):
        """This workflow validate post-migration datasets against their pre-migration counterparts. This includes all
        tables, by comparing their schema, row counts and row comparison
        """
        # need to delete the existing content of recon results table, so that snapshot will re-populate it
        ctx.migration_recon.reset()
        ctx.migration_recon.snapshot()

    @job_task(depends_on=[recon_migration_result], dashboard="migration_main")
    def reconciliation_report(self, ctx: RuntimeContext):
        """Refreshes the migration dashboard after all previous tasks have been completed. Note that you can access the
        dashboard _before_ all tasks have been completed, but then only already completed information is shown."""
