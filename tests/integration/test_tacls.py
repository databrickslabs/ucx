import logging
import os
from functools import partial

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute
from databricks.sdk.service.iam import ComplexValue

from uc_migration_toolkit.providers.client import ImprovedWorkspaceClient
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.providers.mixins.sql import StatementExecutionExt
from uc_migration_toolkit.toolkits.table_acls import TableAclToolkit


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


# _LOG.setLevel("DEBUG")


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


@pytest.fixture
def make_group(ws: ImprovedWorkspaceClient, random):
    cleanup = []

    def inner(
        entitlements: list[ComplexValue] | None = None,
        external_id: str | None = None,
        members: list[ComplexValue] | None = None,
        roles: list[ComplexValue] | None = None,
    ):
        group = ws.groups.create(
            display_name=f"ucx_{random(10)}",
            entitlements=entitlements,
            external_id=external_id,
            members=members,
            roles=roles,
        )
        logger.debug(f"created group fixture: {group.display_name} ({group.id})")
        cleanup.append(group)
        return group

    yield inner

    logger.debug(f"clearing {len(cleanup)} group fixtures")
    for group in cleanup:
        logger.debug(f"removing group fixture: {group.display_name} ({group.id})")
        ws.groups.delete(group.id)


def test_group_fixture(make_group):
    logger.info(f"Created new group: {make_group()}")
    logger.info(f"Created new group: {make_group()}")


@pytest.fixture
def make_schema(hive_exec, random):
    cleanup = []

    def inner():
        name = f"ucx_{random(10)}"
        hive_exec(f"CREATE SCHEMA {name}")
        cleanup.append(name)
        return name

    yield inner
    logger.debug(f"clearing {len(cleanup)} schemas")
    for name in cleanup:
        logger.debug(f"removing {name} schema")
        hive_exec(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    logger.debug(f"removed {len(cleanup)} schemas")


def test_schema_fixture(make_schema):
    logger.info(f"Created new schema: {make_schema()}")
    logger.info(f"Created new schema: {make_schema()}")


@pytest.fixture
def make_table(hive_exec, make_schema, random):
    cleanup = []

    def inner(*, schema: str | None = None, ctas: str | None = None, external: bool = False, view: bool = False):
        if schema is None:
            schema = make_schema()
        name = f"`{schema}`.`ucx_{random(10)}`".lower()
        ddl = f'CREATE {"VIEW" if view else "TABLE"} {name}'
        if ctas is not None:
            # temporary (if not view)
            ddl = f"{ddl} AS {ctas}"
        elif external:
            # external table
            location = "dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled"
            ddl = f"{ddl} USING delta LOCATION '{location}'"
        else:
            # managed table
            ddl = f"{ddl} (id INT, value STRING)"
        hive_exec(ddl)
        cleanup.append(name)
        return name

    yield inner

    logger.debug(f"clearing {len(cleanup)} tables")
    for name in cleanup:
        logger.debug(f"removing {name} table")
        try:
            hive_exec(f"DROP TABLE IF EXISTS {name}")
        except RuntimeError as e:
            if "Cannot drop a view" in str(e):
                hive_exec(f"DROP VIEW IF EXISTS {name}")
            else:
                raise e
    logger.debug(f"removed {len(cleanup)} tables")


def test_table_fixture(make_table):
    logger.info(f"Created new managed table in new schema: {make_table()}")
    logger.info(f'Created new managed table in default schema: {make_table(schema="default")}')
    logger.info(f"Created new external table in new schema: {make_table(external=True)}")
    logger.info(f'Created new tmp table in new schema: {make_table(ctas="SELECT 2+2 AS four")}')
    logger.info(f'Created new view in new schema: {make_table(view=True, ctas="SELECT 2+2 AS four")}')


def test_describe_all_tables(ws: ImprovedWorkspaceClient, make_schema, make_table):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    schema = make_schema()
    managed_table = make_table(schema=schema)
    external_table = make_table(schema=schema, external=True)
    tmp_table = make_table(schema=schema, ctas='SELECT 2+2 AS four')
    view = make_table(schema=schema, ctas='SELECT 2+2 AS four', view=True)

    tak = TableAclToolkit(ws, warehouse_id)

    all = {}
    for t in tak.describe_all_tables_in_database(schema):
        all[f'{t.database}.{t.table}'.lower()] = t

    assert len(all) == 4
    assert all[managed_table].object_type == 'MANAGED'
    assert all[tmp_table].object_type == 'MANAGED'
    assert all[external_table].object_type == 'EXTERNAL'
    assert all[view].object_type == 'VIEW'
    assert all[view].view_text == 'SELECT 2+2 AS four'


def test_all_grants_in_database(ws: ImprovedWorkspaceClient, hive_exec, make_schema, make_table, make_group):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    group_a = make_group()
    group_b = make_group()
    schema = make_schema()
    table = make_table(schema=schema, external=True)

    hive_exec(f"GRANT USAGE ON SCHEMA default TO {group_a.display_name}")
    hive_exec(f"GRANT USAGE ON SCHEMA default TO {group_b.display_name}")
    hive_exec(f"GRANT SELECT ON TABLE {table} TO {group_a.display_name}")
    hive_exec(f"GRANT MODIFY ON SCHEMA {schema} TO {group_b.display_name}")

    tak = TableAclToolkit(ws, warehouse_id)

    all = {}
    for g in tak.all_grants_in_database(schema):
        all[f'{g.principal}.{g.object_key}'.lower()] = g.action_type

    assert len(all) >= 2, 'must have at least two grants'
    assert all[f'{group_a.display_name}.{table}'.lower()] == 'SELECT'
    assert all[f'{group_b.display_name}.{schema}'.lower()] == 'MODIFY'

