from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task


class MigrationRecon(Workflow):
    def __init__(self):
        super().__init__('migration-recon')

    @job_task(job_cluster="main")
    def migration_recon_refresh(self, ctx: RuntimeContext):
        """This workflow validate post-migration datasets against their pre-migration counterparts. This includes all
        tables, by comparing their schema, row counts and row comparison
        """
        ctx.migration_recon.reset()
        ctx.migration_recon.snapshot()
