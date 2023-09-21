import logging
import os

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
