import logging

import pytest

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import TablesMigrate

logger = logging.getLogger(__name__)


@pytest.mark.skip("integration testing environments on AWS do not yet have UC IAM properly setup")
def test_migrate_managed_tables(ws, sql_backend, inventory_schema, make_catalog, make_schema, make_table, env_or_skip):
    src_schema = make_schema(catalog_name="hive_metastore")
    src_managed_table = make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)

    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    logger.info(f"dst_catalog={dst_catalog.name}, managed_table={src_managed_table.full_name}")

    crawler = TablesCrawler(sql_backend, inventory_schema)
    tm = TablesMigrate(crawler, ws, sql_backend, dst_catalog.name)
    tm.migrate_tables()

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_managed_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_managed_table


@pytest.mark.skip("integration testing environments on AWS do not yet have UC IAM properly setup")
def test_migrate_tables_with_cache_should_not_create_table(
    ws, sql_backend, inventory_schema, make_random, make_catalog, make_schema, make_table, env_or_skip
):
    src_schema = make_schema(catalog_name="hive_metastore")

    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    table_name = make_random().lower()
    src_managed_table = make_table(
        catalog_name=src_schema.catalog_name,
        schema_name=src_schema.name,
        name=table_name,
        tbl_properties={"upgraded_from": f"{dst_schema.full_name}.{table_name}"},
    )
    dst_managed_table = make_table(
        catalog_name=dst_schema.catalog_name,
        schema_name=dst_schema.name,
        name=table_name,
        tbl_properties={"upgraded_from": f"{src_schema.full_name}.{table_name}"},
    )

    logger.info(
        f"target_catalog={dst_catalog.name}, "
        f"source_managed_table={src_managed_table}"
        f"target_managed_table={dst_managed_table}"
        f""
    )

    crawler = TablesCrawler(sql_backend, inventory_schema)
    tm = TablesMigrate(crawler, ws, sql_backend, dst_catalog.name)
    tm.migrate_tables()

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
    assert target_tables[0]["database"] == dst_schema.name
    assert target_tables[0]["tableName"] == table_name


@pytest.mark.skip(reason="Needs Storage credential + External Location in place")
def test_migrate_external_table(ws, sql_backend, inventory_schema, make_catalog, make_schema, make_table, env_or_skip):
    src_schema = make_schema(catalog_name="hive_metastore")
    src_external_table = make_table(schema_name=src_schema.name, external=True)

    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, schema_name=src_schema.name)

    logger.info(f"dst_catalog={dst_catalog.name}, external_table={src_external_table.full_name}")

    crawler = TablesCrawler(sql_backend, inventory_schema)
    tm = TablesMigrate(crawler, ws, sql_backend, dst_catalog.name)
    tm.migrate_tables()

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
