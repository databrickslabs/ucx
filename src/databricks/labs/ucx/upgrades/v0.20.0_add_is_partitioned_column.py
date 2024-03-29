# pylint: disable=invalid-name

import logging

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import WorkspaceConfig

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    config = installation.load(WorkspaceConfig)
    warehouse_id = str(config.warehouse_id)
    sql = f"ALTER TABLE {config.inventory_database}.tables ADD COLUMN is_partitioned BOOLEAN"
    ws.statement_execution.execute_statement(sql, warehouse_id=warehouse_id)
    installation.save(config)
