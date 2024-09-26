import datetime as dt
from dataclasses import dataclass


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
