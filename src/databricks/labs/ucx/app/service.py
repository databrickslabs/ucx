"""The UCX operations exposed by the app.

Every operation reuses the same context objects as the matching CLI command, so the app and the CLI behave the same
way. The difference is that the app never blocks on a workflow run: runs are started and their status is polled.
"""

import logging
from datetime import datetime, timezone

from databricks.sdk.errors import InvalidParameterValue
from databricks.sdk.service.jobs import (
    BaseRun,
    Run,
    RunJobTask,
    RunType,
    SubmitRunResponse,
    SubmitTask,
    TaskDependency,
)

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.app.context import AppContext
from databricks.labs.ucx.app.models import (
    ActiveRun,
    ActiveRuns,
    Dashboard,
    Mapping,
    MappingRule,
    RunMigrationRequest,
    MigrationPrecheck,
    MigrationRun,
    Status,
    WorkflowRun,
    WorkflowStatus,
)
from databricks.labs.ucx.hive_metastore.mapping import TableMapping
from databricks.labs.ucx.hive_metastore.tables import What

logger = logging.getLogger(__name__)

ASSESSMENT_WORKFLOW = "assessment"
ASSESSMENT_DASHBOARD = "assessment_main"
MIGRATE_TABLES_WORKFLOW = "migrate-tables"
MIGRATE_HIVESERDE_WORKFLOW = "migrate-external-hiveserde-tables-in-place-experimental"
MIGRATE_CTAS_WORKFLOW = "migrate-external-tables-ctas"
CHAINED_RUN_PREFIX = "[UCX] "


class MappingValidationError(ValueError):
    """The submitted table mapping is not valid."""


class ResourceNotInstalled(Exception):
    """A workflow or dashboard that the app needs is not part of the UCX installation."""


class UcxAppService:
    def __init__(self, ctx: AppContext, auth_mode: str):
        self._ctx = ctx
        self._auth_mode = auth_mode

    @property
    def _host(self) -> str:
        return str(self._ctx.workspace_client.config.host).rstrip("/")

    def status(self) -> Status:
        current_user = self._ctx.workspace_client.current_user.me()
        workflows = [WorkflowStatus(**status) for status in self._ctx.deployed_workflows.latest_job_status()]
        return Status(
            host=self._host,
            user=current_user.user_name or "",
            auth_mode=self._auth_mode,
            install_folder=self._ctx.installation.install_folder(),
            inventory_database=self._ctx.inventory_database,
            ucx_version=__version__,
            workflows=workflows,
        )

    def active_runs(self) -> ActiveRuns:
        """The UCX runs that are in progress: runs of the installed workflows and chained migration runs."""
        ws = self._ctx.jobs_client
        runs = []
        for workflow, job_id in self._ctx.install_state.jobs.items():
            try:
                for run in ws.jobs.list_runs(job_id=int(job_id), active_only=True):
                    runs.append(self._active_run(workflow, run))
            except InvalidParameterValue as e:  # the job was deleted outside of UCX
                logger.warning(f"Skipping {workflow}: {e}")
        for run in ws.jobs.list_runs(run_type=RunType.SUBMIT_RUN, active_only=True):
            if run.run_name and run.run_name.startswith(CHAINED_RUN_PREFIX):
                runs.append(self._active_run(run.run_name.removeprefix(CHAINED_RUN_PREFIX), run))
        return ActiveRuns(runs=runs)

    def run_status(self, run_id: int) -> ActiveRun:
        """The current state of a UCX run, also after it finished. Used to show how a run ended."""
        run = self._ctx.jobs_client.jobs.get_run(run_id)
        workflows_by_job_id = {str(job_id): workflow for workflow, job_id in self._ctx.install_state.jobs.items()}
        if run.job_id is not None and str(run.job_id) in workflows_by_job_id:
            return self._active_run(workflows_by_job_id[str(run.job_id)], run)
        if run.run_name and run.run_name.startswith(CHAINED_RUN_PREFIX):
            return self._active_run(run.run_name.removeprefix(CHAINED_RUN_PREFIX), run)
        # Only UCX runs can be looked up through the app.
        raise ResourceNotInstalled(f"Run {run_id} is not a UCX run")

    def _active_run(self, name: str, run: BaseRun | Run) -> ActiveRun:
        state, state_message = "UNKNOWN", ""
        if run.state:
            if run.state.result_state:  # set once the run finished
                state = run.state.result_state.name
            elif run.state.life_cycle_state:
                state = run.state.life_cycle_state.name
            state_message = run.state.state_message or ""
        started_at = None
        if run.start_time:
            started_at = datetime.fromtimestamp(run.start_time / 1000, tz=timezone.utc).isoformat()
        return ActiveRun(
            name=name,
            run_id=run.run_id or 0,
            state=state,
            state_message=state_message,
            started_at=started_at,
            run_url=run.run_page_url or f"{self._host}/#job/{run.job_id}/runs/{run.run_id}",
        )

    def _requested_by(self) -> str:
        return self._ctx.workspace_client.current_user.me().user_name or "unknown user"

    def _start(self, workflow: str, named_parameters: dict[str, str] | None = None) -> WorkflowRun:
        if workflow not in self._ctx.install_state.jobs:
            raise ResourceNotInstalled(f"Workflow '{workflow}' is not installed. Reinstall UCX to deploy it.")
        run_id = self._ctx.deployed_workflows.run_workflow(
            workflow, skip_job_wait=True, named_parameters=named_parameters
        )
        job_id = self._ctx.install_state.jobs[workflow]
        # The run is triggered by the app's service principal, so record who asked for it.
        logger.info(f"{self._requested_by()} started {workflow}: run {run_id}")
        return WorkflowRun(workflow=workflow, run_id=run_id, run_url=f"{self._host}/#job/{job_id}/runs/{run_id}")

    def run_assessment(self, *, force_refresh: bool) -> WorkflowRun:
        named_parameters = {"force_refresh": "true"} if force_refresh else None
        return self._start(ASSESSMENT_WORKFLOW, named_parameters)

    def assessment_dashboard(self) -> Dashboard:
        dashboard_id = self._ctx.install_state.dashboards.get(ASSESSMENT_DASHBOARD)
        if not dashboard_id:
            raise ResourceNotInstalled(
                "Assessment dashboard not found in the UCX installation. Reinstall UCX to deploy it."
            )
        return Dashboard(
            dashboard_id=dashboard_id,
            url=f"{self._host}/dashboardsv3/{dashboard_id}/published",
            embed_url=f"{self._host}/embed/dashboardsv3/{dashboard_id}",
        )

    def create_mapping(self) -> Mapping:
        self._ctx.table_mapping.save(self._ctx.tables_crawler, self._ctx.workspace_info)
        return self.get_mapping()

    def get_mapping(self) -> Mapping:
        rules = self._ctx.table_mapping.load()
        return Mapping(rules=[MappingRule.from_rule(rule) for rule in rules])

    def save_mapping(self, mapping: Mapping) -> Mapping:
        self._validate_mapping(mapping)
        rules = [rule.to_rule() for rule in mapping.rules]
        self._ctx.installation.save(rules, filename=TableMapping.FILENAME)
        return self.get_mapping()

    @staticmethod
    def _validate_mapping(mapping: Mapping) -> None:
        sources: set[str] = set()
        destinations: set[str] = set()
        errors = []
        for rule in mapping.rules:
            src = f"hive_metastore.{rule.src_schema}.{rule.src_table}".lower()
            dst = f"{rule.catalog_name}.{rule.dst_schema}.{rule.dst_table}".lower()
            if src in sources:
                errors.append(f"Source table mapped more than once: {src}")
            if dst in destinations:
                errors.append(f"Destination table used more than once: {dst}")
            sources.add(src)
            destinations.add(dst)
        if errors:
            raise MappingValidationError("; ".join(errors))

    def migration_precheck(self) -> MigrationPrecheck:
        tables = list(self._ctx.tables_crawler.snapshot())
        prerequisites_error = None
        try:
            # No timeout: a running assessment counts as "not met" instead of blocking the request.
            self._ctx.verify_progress_tracking.verify()
        except RuntimeWarning as e:
            prerequisites_error = str(e)
        return MigrationPrecheck(
            total_tables=len(tables),
            hiveserde_tables=sum(1 for table in tables if table.what == What.EXTERNAL_HIVESERDE),
            external_ctas_tables=sum(1 for table in tables if table.what == What.EXTERNAL_NO_SYNC),
            prerequisites_error=prerequisites_error,
        )

    def run_migration(self, request: RunMigrationRequest) -> MigrationRun:
        """Run the selected migration workflows one after the other, in a single one-time job run.

        The CLI waits for each workflow before starting the next one. The app hands that sequencing to Databricks: one
        run with a `run_job_task` per workflow, each depending on the previous one. The chain then survives app restarts,
        and a failing workflow stops the workflows after it.
        """
        selection = (
            (MIGRATE_TABLES_WORKFLOW, request.migrate_tables),
            (MIGRATE_HIVESERDE_WORKFLOW, request.hiveserde_in_place),
            (MIGRATE_CTAS_WORKFLOW, request.external_ctas),
        )
        workflows = []
        for workflow, selected in selection:
            if selected:
                workflows.append(workflow)
        # Same prerequisites as `databricks labs ucx migrate-tables`: raises RuntimeWarning when not met.
        self._ctx.verify_progress_tracking.verify()
        tasks: list[SubmitTask] = []
        for workflow in workflows:
            job_id = self._ctx.install_state.jobs.get(workflow)
            if not job_id:
                raise ResourceNotInstalled(f"Workflow '{workflow}' is not installed. Reinstall UCX to deploy it.")
            depends_on = [TaskDependency(task_key=tasks[-1].task_key)] if tasks else None
            tasks.append(
                SubmitTask(
                    task_key=workflow.replace("-", "_"),
                    run_job_task=RunJobTask(job_id=int(job_id)),
                    depends_on=depends_on,
                )
            )
        ws = self._ctx.jobs_client
        requested_by = self._requested_by()
        # The run is submitted by the app's service principal, so the name records who asked for it.
        run_name = f"{CHAINED_RUN_PREFIX}{' -> '.join(workflows)} (requested by {requested_by})"
        waiter = ws.jobs.submit(run_name=run_name, tasks=tasks)
        submitted: SubmitRunResponse = waiter.response
        run_id = submitted.run_id
        if not run_id:
            raise RuntimeError("Databricks did not return a run id for the migration run")
        run = ws.jobs.get_run(run_id)
        run_url = run.run_page_url or f"{self._host}/#job/runs/{run_id}"
        logger.info(f"{requested_by} started migration run {run_url}: {', '.join(workflows)}")
        return MigrationRun(run_id=run_id, run_url=run_url, workflows=workflows)
