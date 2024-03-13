# pylint: disable=invalid-name

import logging
from typing import Any

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.assessment.clusters import ClusterInfo
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import (
    SchemaDeployer,
    StatementExecutionBackend,
)

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    config = installation.load(WorkspaceConfig)
    sql = f"DROP TABLE IF EXISTS hive_metastore.{config.inventory_database}.clusters"
    sql_backend = StatementExecutionBackend(ws=ws, warehouse_id=config.warehouse_id)
    sql_backend.execute(sql=sql)
    deploy_schema = SchemaDeployer(sql_backend=sql_backend, inventory_schema=config.inventory_database, mod=Any)
    deploy_schema.deploy_table("clusters", ClusterInfo)
