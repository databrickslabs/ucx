# pylint: disable=invalid-name

import logging

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import WorkspaceConfig

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    config = installation.load(WorkspaceConfig)
    sql = f"ALTER TABLE hive_metastore.{config.inventory_database}.clusters ADD COLUMNS(policy_id string,spark_version string)"
    sql_backend = StatementExecutionBackend(ws=ws, warehouse_id=config.warehouse_id)
    sql_backend.execute(sql=sql)
