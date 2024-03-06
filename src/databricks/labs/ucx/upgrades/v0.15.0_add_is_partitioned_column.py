from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import WorkspaceConfig

from src.databricks.labs.ucx.framework.crawlers import StatementExecutionBackend


def upgrade(installation: Installation, ws: WorkspaceClient):
    config = installation.load(WorkspaceConfig)
    sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
    sql_backend.execute(f"ALTER TABLE {config.inventory_database}.tables ADD COLUMN is_partitioned BOOLEAN")
    installation.save(config)