import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import What

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_describe_all_tables_in_databases(ws, sql_backend, inventory_schema, make_schema, make_table, env_or_skip):
    logger.info("setting up fixtures")

    schema_a = make_schema(catalog_name="hive_metastore")
    schema_b = make_schema(catalog_name="hive_metastore")
    make_schema(catalog_name="hive_metastore")

    managed_table = make_table(schema_name=schema_a.name)
    external_table = make_table(schema_name=schema_b.name, external=True)
    tmp_table = make_table(schema_name=schema_a.name, ctas="SELECT 2+2 AS four")
    view = make_table(schema_name=schema_b.name, ctas="SELECT 2+2 AS four", view=True)
    non_delta = make_table(schema_name=schema_a.name, non_delta=True)

    logger.info(
        f"managed_table={managed_table.full_name}, "
        f"external_table={external_table.full_name}, "
        f"tmp_table={tmp_table.full_name}, "
        f"view={view.full_name}"
    )

    schema_c = make_schema(catalog_name="hive_metastore")
    make_table(schema_name=schema_c.name)

    tables = TablesCrawler(sql_backend, inventory_schema, [schema_a.name, schema_b.name])

    all_tables = {}
    for table in tables.snapshot():
        all_tables[table.key] = table

    assert len(all_tables) == 5
    assert all_tables[non_delta.full_name].table_format == "JSON"
    assert all_tables[non_delta.full_name].what == What.DB_DATASET
    assert all_tables[managed_table.full_name].object_type == "MANAGED"
    assert all_tables[managed_table.full_name].what == What.DBFS_ROOT_DELTA
    assert all_tables[tmp_table.full_name].object_type == "MANAGED"
    assert all_tables[tmp_table.full_name].what == What.DBFS_ROOT_DELTA
    assert all_tables[external_table.full_name].object_type == "EXTERNAL"
    assert all_tables[external_table.full_name].what == What.EXTERNAL_NO_SYNC
    assert all_tables[view.full_name].object_type == "VIEW"
    assert all_tables[view.full_name].view_text == "SELECT 2+2 AS four"
    assert all_tables[view.full_name].what == What.VIEW
