import os
from dataclasses import dataclass
from typing import Type

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute

from uc_migration_toolkit.providers.client import ImprovedWorkspaceClient
from uc_migration_toolkit.providers.logger import logger


@dataclass
class ShowTable:
    database: str
    tableName: str
    isTemporary: bool


class CommandExecutor:
    def __init__(self, ws: WorkspaceClient, cluster_id):
        self.cluster_id = cluster_id
        self.ws = ws
        self.ctx = ws.command_execution.create(
            cluster_id=cluster_id,
            language=compute.Language.PYTHON).result()

    def run(self, command: str, language=compute.Language.PYTHON):
        return self.ws.command_execution.execute(
            cluster_id=self.cluster_id,
            context_id=self.ctx.id,
            language=language,
            command=command,
        ).result()

    def sql(self, query: str, result_type: Type = None):
        logger.debug(f'Running SQL query on {self.cluster_id}: {query}')
        res = self.run(query, language=compute.Language.SQL)
        results = res.results
        if result_type is None:
            schema_str = ', '.join([f'{c["name"]}:{c["type"]}' for c in results.schema])
            raise ValueError(f'Needs constructor with schema: {schema_str}')
        if results.result_type == compute.ResultType.TABLE:
            for tpl in results.data:
                yield result_type(*tpl)
        else:
            raise RuntimeError(f'Unknown result type: {results.result_type}: {results.summary}')


def test_table_acls(ws: ImprovedWorkspaceClient, random):
    cluster_id = os.environ['TABLE_ACL_CLUSTER_ID']
    ce = CommandExecutor(ws, cluster_id)

    table_name = f'ucx_{random(12)}'
    ce.sql(f'CREATE TABLE {table_name} AS SELECT 2+2 AS four, 2+3 AS five')

    for t in ce.sql("SHOW TABLES", ShowTable):
        print(t)
