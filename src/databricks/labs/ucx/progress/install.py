import datetime as dt
import logging
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.deployment import SchemaDeployer
from databricks.labs.ucx.progress.workflow_runs import WorkflowRun

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class HistoricalRecord:  # pylint: disable=too-many-instance-attributes
    workspace_id: int
    """The identifier of the workspace where this record was generated."""

    run_id: int
    """An identifier of the workflow run that generated this record."""

    run_as: str
    """The identity of the account that ran the workflow that generated this record."""

    run_start_time: dt.datetime
    """When this record was generated."""

    snapshot_id: int
    """An identifier that is unique to the records produced for a given snapshot."""

    ucx_version: str
    """The UCX semantic version."""

    failures: list[str]
    """The list of problems associated with the object that this inventory record covers."""

    object_type: str
    """The inventory table for which this record was generated."""

    object_id: list[str]
    """The type-specific identifier for this inventory record."""

    object_data: str
    """Type-specific JSON-encoded data of the inventory record."""

    owner: str
    """The identity that has ownership of the object."""

    object_type_version: int = 0
    """Versioning of inventory table, for forward compatibility."""


class ProgressTrackingInstallation:
    """Install resources for UCX's progress tracking."""

    _SCHEMA = "multiworkspace"

    def __init__(self, sql_backend: SqlBackend, ucx_catalog: str) -> None:
        # `mod` is a required parameter, though, it's not used in this context without views.
        self._schema_deployer = SchemaDeployer(sql_backend, self._SCHEMA, mod=None, catalog=ucx_catalog)

    def run(self) -> None:
        self._schema_deployer.deploy_schema()
        self._schema_deployer.deploy_table("workflow_runs", WorkflowRun)
        self._schema_deployer.deploy_table("historical_records", HistoricalRecord)
        logger.info("Installation completed successfully!")
