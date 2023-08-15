import logging
import os
from dataclasses import dataclass
from functools import partial

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute

from uc_migration_toolkit.providers.client import ImprovedWorkspaceClient
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.providers.mixins.sql import StatementExecutionExt
from uc_migration_toolkit.toolkits.table_acls import TableAclToolkit


@dataclass
class ShowTable:
    database: str
    tableName: str
    isTemporary: bool


class CommandExecutor:
    def __init__(self, ws: WorkspaceClient, cluster_id):
        self.cluster_id = cluster_id
        self.ws = ws
        self.ctx = ws.command_execution.create(cluster_id=cluster_id, language=compute.Language.PYTHON).result()

    def run(self, command: str, language=compute.Language.PYTHON):
        return self.ws.command_execution.execute(
            cluster_id=self.cluster_id,
            context_id=self.ctx.id,
            language=language,
            command=command,
        ).result()

    def sql(self, query: str, result_type: type | None = None):
        logger.debug(f"Running SQL query on {self.cluster_id}: {query}")
        res = self.run(query, language=compute.Language.SQL)
        results = res.results
        if result_type is None:
            schema_str = ", ".join([f'{c["name"]}:{c["type"]}' for c in results.schema])
            msg = f"Needs constructor with schema: {schema_str}"
            raise ValueError(msg)
        if results.result_type == compute.ResultType.TABLE:
            for tpl in results.data:
                yield result_type(*tpl)
        else:
            msg = f"Unknown result type: {results.result_type}: {results.summary}"
            raise RuntimeError(msg)


_LOG = logging.getLogger("databricks.sdk")


_LOG.setLevel("DEBUG")


@pytest.fixture
def hive_exec(ws: ImprovedWorkspaceClient):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]
    statement_execution = StatementExecutionExt(ws.api_client)
    return partial(statement_execution.execute, warehouse_id, catalog="hive_metastore")


@pytest.fixture
def hive_fetch_all(ws: ImprovedWorkspaceClient):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]
    statement_execution = StatementExecutionExt(ws.api_client)
    return partial(statement_execution.execute_fetch_all, warehouse_id, catalog="hive_metastore")


def test_sql_exec(ws: ImprovedWorkspaceClient, hive_fetch_all, hive_exec, random):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    table_name = f"ucx_{random(12)}"
    hive_exec(f"CREATE TABLE {table_name} AS SELECT 2+2 AS four")
    print(table_name)

    tmp_group = ws.groups.create(display_name=f"ucx_{random(10)}")

    hive_exec(f"GRANT USAGE ON SCHEMA default TO {tmp_group.display_name}")
    hive_exec(f"GRANT SELECT ON TABLE default.{table_name} TO {tmp_group.display_name}")

    tak = TableAclToolkit(ws, warehouse_id)

    tak.describe("default", table_name)

    # for x in tak.grants(f'TABLE default.{table_name}'):
    #     print(x)

    hive_exec(f"DROP TABLE {table_name}")
    ws.groups.delete(tmp_group.id)


def test_table_acls(ws: ImprovedWorkspaceClient, random):
    cluster_id = os.environ["TABLE_ACL_CLUSTER_ID"]
    ce = CommandExecutor(ws, cluster_id)

    table_name = f"ucx_{random(12)}"
    ce.sql(f"CREATE TABLE {table_name} AS SELECT 2+2 AS four, 2+3 AS five")

    for t in ce.sql("SHOW TABLES", ShowTable):
        print(t)


def test_create_spn(ws):
    spn = ws.service_principals.create(display_name="simplest-spn")
    obo_token = ws.token_management.create_obo_token(spn.application_id, 60 * 60 * 24 * 30)
    print(
        f"""[labs-aws-{spn.display_name}]
    host={ws.config.host}
    token={obo_token.token_value}
    """
    )
