import datetime as dt
import logging
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.deployment import SchemaDeployer


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

    run_as: str
    """The identity the workflow was run as`"""

    status: str
    """The workflow run final status"""


class ProgressTrackingInstaller:
    """Install resources for UCX's progress tracking."""

    _SCHEMA = "history"

    def __init__(self, sql_backend: SqlBackend, ucx_catalog: str) -> None:
        # `mod` is required parameter, but it's not used in this context.
        self._schema_deployer = SchemaDeployer(sql_backend, self._SCHEMA, mod=None, catalog=ucx_catalog)

    def run(self) -> None:
        self._schema_deployer.deploy_schema()
        self._schema_deployer.deploy_table("workflow_runs", WorkflowRun)
        logger.info("Installation completed successfully!")
