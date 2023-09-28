import logging
import os

import pytest

from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import TablesMigrate

logger = logging.getLogger(__name__)


def test_migrate_managed_tables(ws, make_catalog, make_schema, make_table):
    target_catalog = make_catalog()
    schema_a = make_schema(catalog="hive_metastore")
    _, target_schema = schema_a.split(".")

    make_schema(catalog=target_catalog, schema=target_schema)

    managed_table = make_table(schema=schema_a)

    logger.info(f"target_catalog={target_catalog}, managed_table={managed_table}")

    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")

    backend = StatementExecutionBackend(ws, os.environ["TEST_DEFAULT_WAREHOUSE_ID"])
    crawler = TablesCrawler(backend, inventory_schema)
    tm = TablesMigrate(tc=crawler, ws=ws, backend=backend, default_catalog=target_catalog)
    tm.migrate_tables()

    target_tables = list(backend.fetch(f"SHOW TABLES IN {target_catalog}.{target_schema}"))
    assert len(target_tables) == 1

    _, _, managed_table_name = managed_table.split(".")
    target_table_properties = ws.tables.get(f"{target_catalog}.{target_schema}.{managed_table_name}").properties

    assert target_table_properties["upgraded_from"] == managed_table


def test_migrate_tables_with_cache_should_not_create_table(ws, make_random, make_catalog, make_schema, make_table):
    target_catalog = make_catalog()
    schema_a = make_schema(catalog="hive_metastore")
    _, target_schema = schema_a.split(".")

    make_schema(catalog=target_catalog, schema=target_schema)

    table_name = make_random().lower()
    target_table = f"{target_catalog}.{target_schema}.{table_name}"
    source_table = f"hive_metastore.{target_schema}.{table_name}"
    target_managed_table = make_table(
        name=target_table, tbl_properties=f"TBLPROPERTIES ('upgraded_from' = '{source_table}')"
    )
    source_managed_table = make_table(
        name=source_table, tbl_properties=f"TBLPROPERTIES ('upgraded_from' = '{target_table}')"
    )

    logger.info(
        f"target_catalog={target_catalog}, "
        f"source_managed_table={source_managed_table}"
        f"target_managed_table={target_managed_table}"
        f""
    )

    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")

    backend = StatementExecutionBackend(ws, os.environ["TEST_DEFAULT_WAREHOUSE_ID"])
    crawler = TablesCrawler(backend, inventory_schema)
    tm = TablesMigrate(tc=crawler, ws=ws, backend=backend, default_catalog=target_catalog)
    tm.migrate_tables()

    target_tables = list(backend.fetch(f"SHOW TABLES IN {target_catalog}.{target_schema}"))
    assert len(target_tables) == 1
    assert target_tables[0]["database"] == target_schema
    assert target_tables[0]["tableName"] == table_name


@pytest.mark.skip(reason="Needs Storage credential + External Location in place")
def test_migrate_external_table(ws, make_catalog, make_schema, make_table):
    target_catalog = make_catalog()
    schema_a = make_schema(catalog="hive_metastore")
    _, target_schema = schema_a.split(".")

    make_schema(catalog=target_catalog, schema=target_schema)

    external_table = make_table(schema=schema_a, external=True)

    logger.info(f"target catalog={target_catalog}, external_table={external_table} ")

    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")

    backend = StatementExecutionBackend(ws, os.environ["TEST_DEFAULT_WAREHOUSE_ID"])

    backend = StatementExecutionBackend(ws, os.environ["TEST_DEFAULT_WAREHOUSE_ID"])
    crawler = TablesCrawler(backend, inventory_schema)
    tm = TablesMigrate(crawler, ws, backend, target_catalog, inventory_schema)
    tm.migrate_tables()

    target_tables = list(backend.fetch(f"SHOW TABLES IN {target_catalog}.{target_schema}"))
    assert len(target_tables) == 1
