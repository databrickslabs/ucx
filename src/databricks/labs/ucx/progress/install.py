import logging
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.deployment import SchemaDeployer

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.progress.workflow_runs import WorkflowRun


logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class Historical:
    workspace_id: int
    """The identifier of the workspace where this historical record was generated."""

    job_run_id: int
    """The identifier of the job run that generated this historical record."""

    object_type: str
    """The inventory table for which this historical record was generated."""

    object_id: list[str]
    """The type-specific identifier for the corresponding inventory record."""

    data: dict[str, str]
    """Type-specific JSON-encoded data of the inventory record."""

    failures: list[str]
    """The list of problems associated with the object that this inventory record covers."""

    owner: str
    """The identity that has ownership of the object."""

    ucx_version: str = __version__
    """The UCX semantic version."""


class ProgressTrackingInstallation:
    """Install resources for UCX's progress tracking."""

    _SCHEMA = "multiworkspace"

    def __init__(self, sql_backend: SqlBackend, ucx_catalog: str) -> None:
        # `mod` is a required parameter, though, it's not used in this context without views.
        self._schema_deployer = SchemaDeployer(sql_backend, self._SCHEMA, mod=None, catalog=ucx_catalog)

    def run(self) -> None:
        self._schema_deployer.deploy_schema()
        self._schema_deployer.deploy_table("workflow_runs", WorkflowRun)
        self._schema_deployer.deploy_table("historical", Historical)
        logger.info("Installation completed successfully!")
