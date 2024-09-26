import datetime as dt
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient


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

    run_as: str
    """The identity the workflow was run as`"""

    status: str
    """The workflow run final status"""


class WorkflowRunRecorder:
    """Record workflow runs in a database."""

    def __init__(
        self,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
        *,
        workflow_name: str,
        workflow_id: int,
        workflow_run_id: int,
        attempt: int,
    ):
        self._ws = ws
        self._sql_backend = sql_backend
        self._full_table_name = f"{self._catalog}.multiworkspace.workflow_runs"
        self._workflow_name = workflow_name
        self._workflow_id = workflow_id
        self._workflow_run_id = workflow_run_id

    def record(self, *, start_time: dt.datetime) -> None:
        """Record a workflow run in the database."""
        workflow_run = WorkflowRun(
            started_at=start_time,
            finished_at=dt.datetime.now(),
            workspace_id=self._ws.get_workspace_id(),
            workflow_name=self._workflow_name,
            workflow_id=self._workflow_id,
            workflow_run_id=self._workflow_run_id,
            run_as="UNKOWN",  # TODO Update this,
            status="RUNNING",  # Always RUNNING as it is called during a running workflow
        )
        self._sql_backend.save_table(
            self._full_table_name,
            [workflow_run],
            WorkflowRun,
            mode="append",
        )
