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

    @job_task(job_cluster="table_migration")
    def lint_all_queries(self, ctx: RuntimeContext):
        """[EXPERIMENTAL] Analyses all jobs for source code compatibility problems. This is an experimental feature,
        that is not yet fully supported."""
        ctx.query_linter.refresh_report(ctx.sql_backend, ctx.inventory_database)
