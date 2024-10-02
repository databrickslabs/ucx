import datetime as dt
import logging
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import NotFound


logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class WorkflowRun:
    started_at: dt.datetime
    """The timestamp of the workflow run start."""

    finished_at: dt.datetime
    """The timestamp of the workflow run end."""

    workspace_id: int
    """The workspace id in which the workflow ran."""

    workflow_name: str
    """The workflow name that ran."""

    workflow_id: int
    """"The workflow id of the workflow that ran."""

    workflow_run_id: int
    """The workflow run id."""

    workflow_run_attempt: int
    """The workflow run attempt."""


class WorkflowRunRecorder:
    """Record workflow runs in a database."""

    def __init__(
        self,
        sql_backend: SqlBackend,
        ucx_catalog: str,
        *,
        workspace_id: int,
        workflow_name: str,
        workflow_id: int,
        workflow_run_id: int,
        workflow_run_attempt: int,
        workflow_start_time: str,
    ):
        self._sql_backend = sql_backend
        self._full_table_name = f"{ucx_catalog}.multiworkspace.workflow_runs"
        self._workspace_id = workspace_id
        self._workflow_name = workflow_name
        self._workflow_start_time = workflow_start_time
        self._workflow_id = workflow_id
        self._workflow_run_id = workflow_run_id
        self._workflow_run_attempt = workflow_run_attempt

    def record(self) -> None:
        """Record a workflow run."""
        workflow_run = WorkflowRun(
            started_at=dt.datetime.fromisoformat(self._workflow_start_time),
            finished_at=dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0),
            workspace_id=self._workspace_id,
            workflow_name=self._workflow_name,
            workflow_id=self._workflow_id,
            workflow_run_id=self._workflow_run_id,
            workflow_run_attempt=self._workflow_run_attempt,
        )
        try:
            self._sql_backend.save_table(
                self._full_table_name,
                [workflow_run],
                WorkflowRun,
                mode="append",
            )
        except NotFound as e:
            logger.error(f"Workflow run table not found: {self._full_table_name}", exc_info=e)
