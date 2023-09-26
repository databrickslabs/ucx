import logging
import os

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore import TablesCrawler

logger = logging.getLogger(__name__)


def test_describe_all_tables_in_databases(ws: WorkspaceClient, make_catalog, make_schema, make_table):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    logger.info("setting up fixtures")

    schema_a = make_schema(catalog="hive_metastore")

    schema_b = make_schema(catalog="hive_metastore")

    make_schema(catalog="hive_metastore")

    managed_table = make_table(schema=schema_a)
    external_table = make_table(schema=schema_b, external=True)
    tmp_table = make_table(schema=schema_a, ctas="SELECT 2+2 AS four")
    view = make_table(schema=schema_b, ctas="SELECT 2+2 AS four", view=True)
    non_delta = make_table(schema=schema_a, non_delta=True)

    logger.info(
        f"managed_table={managed_table}, "
        f"external_table={external_table}, "
        f"tmp_table={tmp_table}, "
        f"view={view}"
    )

    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")

    backend = StatementExecutionBackend(ws, warehouse_id)
    tables = TablesCrawler(backend, inventory_schema)

    all_tables = {}
    for t in tables.snapshot():
        all_tables[t.key] = t

    assert len(all_tables) >= 5
    assert all_tables[non_delta].table_format == "JSON"
    assert all_tables[managed_table].object_type == "MANAGED"
    assert all_tables[tmp_table].object_type == "MANAGED"
    assert all_tables[external_table].object_type == "EXTERNAL"
    assert all_tables[view].object_type == "VIEW"
    assert all_tables[view].view_text == "SELECT 2+2 AS four"


def test_migrate_view_and_managed_tables(ws, make_catalog, make_schema, make_table):
    target_catalog = make_catalog()
    schema_a = make_schema(catalog="hive_metastore")
    _, target_schema = schema_a.split(".")

    make_schema(catalog=target_catalog, schema_name=target_schema)

    managed_table = make_table(schema=schema_a)
    view = make_table(schema=schema_a, ctas="SELECT 2+2 AS four", view=True)

    logger.info(f"target catalog={target_catalog}, managed_table={managed_table}, view={view}")

    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")

    backend = StatementExecutionBackend(ws, os.environ["TEST_DEFAULT_WAREHOUSE_ID"])

    tables = TablesCrawler(backend, inventory_schema)
    tables.snapshot()
    tables.migrate_tables(target_catalog)

    target_tables = list(backend.fetch(f"SHOW TABLES IN {target_catalog}.{target_schema}"))
    assert len(target_tables) == 2

    _, _, view_name = view.split(".")
    view_property = ""
    for detail in list(backend.fetch(f"DESCRIBE EXTENDED {view}")):
        if detail["col_name"] == "Table Properties":
            view_property = detail["data_type"]
    assert f"upgraded_to={target_catalog}.{target_schema}.{view_name}" in view_property

    managed_table_property = ""
    _, _, managed_table_name = managed_table.split(".")
    for detail in list(backend.fetch(f"DESCRIBE EXTENDED {managed_table}")):
        if detail["col_name"] == "Table Properties":
            managed_table_property = detail["data_type"]
    assert f"upgraded_to={target_catalog}.{target_schema}.{managed_table_name}" in managed_table_property


@pytest.mark.skip(reason="Needs Storage credential + External Location in place")
def test_migrate_external_table(ws, make_catalog, make_schema, make_table):
    target_catalog = make_catalog()
    schema_a = make_schema(catalog="hive_metastore")
    _, target_schema = schema_a.split(".")

    make_schema(catalog=target_catalog, schema_name=target_schema)

    external_table = make_table(schema=schema_a, external=True)

    logger.info(f"target catalog={target_catalog}, external_table={external_table} ")

    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")

    backend = StatementExecutionBackend(ws, os.environ["TEST_DEFAULT_WAREHOUSE_ID"])

    tables = TablesCrawler(backend, inventory_schema)
    tables.snapshot()
    tables.migrate_tables(target_catalog)

    target_tables = list(backend.fetch(f"SHOW TABLES IN {target_catalog}.{target_schema}"))
    assert len(target_tables) == 1
