import logging

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.deployment import SchemaDeployer
from databricks.labs.ucx.progress.workflow_runs import WorkflowRun

logger = logging.getLogger(__name__)


class ProgressTrackingInstallation:
    """Install resources for UCX's progress tracking."""

    _SCHEMA = "multiworkspace"

    def __init__(self, sql_backend: SqlBackend, ucx_catalog: str) -> None:
        # `mod` is a required parameter, though, it's not used in this context without views.
        self._schema_deployer = SchemaDeployer(sql_backend, self._SCHEMA, mod=None, catalog=ucx_catalog)

    def run(self) -> None:
        self._schema_deployer.deploy_schema()
        self._schema_deployer.deploy_table("workflow_runs", WorkflowRun)
        logger.info("Installation completed successfully!")
