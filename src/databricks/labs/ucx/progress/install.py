import logging

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.deployment import SchemaDeployer


logger = logging.getLogger(__name__)


class HistoryInstallation:
    """Install resources for UCX's artifacts history."""

    _SCHEMA = "history"

    def __init__(self, sql_backend: SqlBackend, ucx_catalog: str) -> None:
        # `mod` is required parameter, but it's not used in this context.
        self._schema_deployer = SchemaDeployer(sql_backend, self._SCHEMA, mod=None, catalog=ucx_catalog)

    def run(self) -> None:
        self._schema_deployer.deploy_schema()
        logger.info("Installation completed successfully!")
