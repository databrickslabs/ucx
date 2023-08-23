import os
from functools import partial

import pytest
from databricks.sdk.service.iam import ComplexValue

from databricks.labs.ucx.providers.client import ImprovedWorkspaceClient
from databricks.labs.ucx.providers.logger import logger
from databricks.labs.ucx.providers.mixins.sql import StatementExecutionExt
from databricks.labs.ucx.toolkits.table_acls import TaclToolkit

# _LOG.setLevel("DEBUG")


@pytest.fixture
def sql_exec(ws: ImprovedWorkspaceClient):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]
    statement_execution = StatementExecutionExt(ws.api_client)
    return partial(statement_execution.execute, warehouse_id)


@pytest.fixture
def sql_fetch_all(ws: ImprovedWorkspaceClient):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]
    statement_execution = StatementExecutionExt(ws.api_client)
    return partial(statement_execution.execute_fetch_all, warehouse_id)


@pytest.fixture
def make_group(ws: ImprovedWorkspaceClient, make_random):
    cleanup = []

    def inner(
        entitlements: list[ComplexValue] | None = None,
        external_id: str | None = None,
        members: list[ComplexValue] | None = None,
        roles: list[ComplexValue] | None = None,
    ):
        group = ws.groups.create(
            display_name=f"ucx_G{make_random(4)}",
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
def make_catalog(sql_exec, make_random):
    cleanup = []

    def inner():
        name = f"ucx_C{make_random(4)}".lower()
        sql_exec(f"CREATE CATALOG {name}")
        cleanup.append(name)
        return name

    yield inner
    logger.debug(f"clearing {len(cleanup)} catalogs")
    for name in cleanup:
        logger.debug(f"removing {name} catalog")
        sql_exec(f"DROP CATALOG IF EXISTS {name} CASCADE")
    logger.debug(f"removed {len(cleanup)} catalogs")


def test_catalog_fixture(make_catalog):
    logger.info(f"Created new catalog: {make_catalog()}")
    logger.info(f"Created new catalog: {make_catalog()}")


@pytest.fixture
def make_schema(sql_exec, make_random):
    cleanup = []

    def inner(catalog="hive_metastore"):
        name = f"{catalog}.ucx_S{make_random(4)}".lower()
        sql_exec(f"CREATE SCHEMA {name}")
        cleanup.append(name)
        return name

    yield inner
    logger.debug(f"clearing {len(cleanup)} schemas")
    for name in cleanup:
        logger.debug(f"removing {name} schema")
        sql_exec(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    logger.debug(f"removed {len(cleanup)} schemas")


def test_schema_fixture(make_schema):
    logger.info(f"Created new schema: {make_schema()}")
    logger.info(f"Created new schema: {make_schema()}")


@pytest.fixture
def make_table(sql_exec, make_schema, make_random):
    cleanup = []

    def inner(
        *,
        catalog="hive_metastore",
        schema: str | None = None,
        ctas: str | None = None,
        non_detla: bool = False,
        external: bool = False,
        view: bool = False,
    ):
        if schema is None:
            schema = make_schema(catalog=catalog)
        name = f"{schema}.ucx_T{make_random(4)}".lower()
        ddl = f'CREATE {"VIEW" if view else "TABLE"} {name}'
        if ctas is not None:
            # temporary (if not view)
            ddl = f"{ddl} AS {ctas}"
        elif non_detla:
            location = "dbfs:/databricks-datasets/iot-stream/data-device"
            ddl = f"{ddl} USING json LOCATION '{location}'"
        elif external:
            # external table
            location = "dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled"
            ddl = f"{ddl} USING delta LOCATION '{location}'"
        else:
            # managed table
            ddl = f"{ddl} (id INT, value STRING)"
        sql_exec(ddl)
        cleanup.append(name)
        return name

    yield inner

    logger.debug(f"clearing {len(cleanup)} tables")
    for name in cleanup:
        logger.debug(f"removing {name} table")
        try:
            sql_exec(f"DROP TABLE IF EXISTS {name}")
        except RuntimeError as e:
            if "Cannot drop a view" in str(e):
                sql_exec(f"DROP VIEW IF EXISTS {name}")
            else:
                raise e
    logger.debug(f"removed {len(cleanup)} tables")


def test_table_fixture(make_table):
    logger.info(f"Created new managed table in new schema: {make_table()}")
    logger.info(f'Created new managed table in default schema: {make_table(schema="default")}')
    logger.info(f"Created new external table in new schema: {make_table(external=True)}")
    logger.info(f"Created new external JSON table in new schema: {make_table(non_detla=True)}")
    logger.info(f'Created new tmp table in new schema: {make_table(ctas="SELECT 2+2 AS four")}')
    logger.info(f'Created new view in new schema: {make_table(view=True, ctas="SELECT 2+2 AS four")}')


def test_describe_all_tables(ws: ImprovedWorkspaceClient, make_catalog, make_schema, make_table):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    logger.info("setting up fixtures")
    schema = make_schema(catalog="hive_metastore")
    managed_table = make_table(schema=schema)
    external_table = make_table(schema=schema, external=True)
    tmp_table = make_table(schema=schema, ctas="SELECT 2+2 AS four")
    view = make_table(schema=schema, ctas="SELECT 2+2 AS four", view=True)
    non_delta = make_table(schema=schema, non_detla=True)

    logger.info(
        f"managed_table={managed_table}, "
        f"external_table={external_table}, "
        f"tmp_table={tmp_table}, "
        f"view={view}"
    )

    inventory_schema = make_schema(catalog=make_catalog())
    inventory_catalog, inventory_schema = inventory_schema.split(".")
    tak = TaclToolkit(ws, warehouse_id, inventory_catalog, inventory_schema)

    all_tables = {}
    for t in tak.database_snapshot(schema.split(".")[1]):
        all_tables[t.key] = t

    assert len(all_tables) == 5
    assert all_tables[non_delta].table_format == "JSON"
    assert all_tables[managed_table].object_type == "MANAGED"
    assert all_tables[tmp_table].object_type == "MANAGED"
    assert all_tables[external_table].object_type == "EXTERNAL"
    assert all_tables[view].object_type == "VIEW"
    assert all_tables[view].view_text == "SELECT 2+2 AS four"


def test_all_grants_in_database(
    ws: ImprovedWorkspaceClient, sql_exec, make_catalog, make_schema, make_table, make_group
):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    group_a = make_group()
    group_b = make_group()
    schema = make_schema()
    table = make_table(schema=schema, external=True)

    sql_exec(f"GRANT USAGE ON SCHEMA default TO {group_a.display_name}")
    sql_exec(f"GRANT USAGE ON SCHEMA default TO {group_b.display_name}")
    sql_exec(f"GRANT SELECT ON TABLE {table} TO {group_a.display_name}")
    sql_exec(f"GRANT MODIFY ON SCHEMA {schema} TO {group_b.display_name}")

    inventory_schema = make_schema(catalog=make_catalog())
    inventory_catalog, inventory_schema = inventory_schema.split(".")
    tak = TaclToolkit(ws, warehouse_id, inventory_catalog, inventory_schema)

    all_grants = {}
    for grant in tak.grants_snapshot(schema.split(".")[1]):
        logger.info(f"grant:\n{grant}\n  hive: {grant.hive_grant_sql()}\n  uc: {grant.uc_grant_sql()}")
        all_grants[f"{grant.principal}.{grant.object_key}"] = grant.action_type

    assert len(all_grants) >= 2, "must have at least two grants"
    assert all_grants[f"{group_a.display_name}.{table}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{schema}"] == "MODIFY"
