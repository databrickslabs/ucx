from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task


class ExperimentalWorkflowLinter(Workflow):
    def __init__(self):
        super().__init__('experimental-workflow-linter')

    @job_task(job_cluster="table_migration")
    def lint_all_workflows(self, ctx: RuntimeContext):
        """[EXPERIMENTAL] Analyses all jobs for source code compatibility problems. This is an experimental feature,
        that is not yet fully supported."""
        ctx.workflow_linter.refresh_report(ctx.sql_backend, ctx.inventory_database)

    @job_task(dashboard="migration_main", depends_on=[lint_all_workflows])
    def migration_report(self, ctx: RuntimeContext):
        """Refreshes the migration dashboard after all previous tasks have been completed. Note that you can access the
        dashboard _before_ all tasks have been completed, but then only already completed information is shown."""
