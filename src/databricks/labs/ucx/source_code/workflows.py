from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.framework.tasks import Workflow, job_task


class WorkflowLinter(Workflow):
    def __init__(self):
        super().__init__('workflow-linter')

    @job_task
    def assess_workflows(self, ctx: RuntimeContext):
        """Scans all jobs for migration issues in notebooks jobs.

        Also, stores direct filesystem accesses for display in the migration dashboard.
        """
        ctx.workflow_linter.refresh_report(ctx.sql_backend, ctx.inventory_database)

    @job_task
    def assess_dashboards(self, ctx: RuntimeContext):
        """Scans all dashboards for migration issues in SQL code of embedded widgets.

        Also, stores direct filesystem accesses for display in the migration dashboard.
        """
        ctx.query_linter.refresh_report(ctx.sql_backend, ctx.inventory_database)
