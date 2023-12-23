import logging
from datetime import timedelta

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import TableInfo

from databricks.labs.ucx.hive_metastore.tables import TablesMigrate, TablesCrawler
from databricks.labs.ucx.mixins.sql import Row

from ..conftest import StaticTablesCrawler

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_managed_tables(ws, sql_backend, inventory_schema, make_catalog, make_schema, make_table):
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")
    src_schema = make_schema(catalog_name="hive_metastore")
    src_managed_table = make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)

    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    logger.info(f"dst_catalog={dst_catalog.name}, managed_table={src_managed_table.full_name}")

    # crawler = TablesCrawler(sql_backend, inventory_schema)
    crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_managed_table])
    tm = TablesMigrate(crawler, ws, sql_backend, dst_catalog.name)
    tm.migrate_tables()

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_managed_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_managed_table.full_name


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_migrate_tables_with_cache_should_not_create_table(
    ws, sql_backend, inventory_schema, make_random, make_catalog, make_schema, make_table
):
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")
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
    )

    # crawler = TablesCrawler(sql_backend, inventory_schema)
    crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_managed_table])
    tm = TablesMigrate(crawler, ws, sql_backend, dst_catalog.name)

    # FIXME: flaky: databricks.sdk.errors.mapping.NotFound: Catalog 'ucx_cjazg' does not exist.
    tm.migrate_tables()

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
    assert target_tables[0]["database"] == dst_schema.name
    assert target_tables[0]["tableName"] == table_name


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_migrate_external_table(ws, sql_backend, inventory_schema, make_catalog, make_schema, make_table, env_or_skip):
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")
    src_schema = make_schema(catalog_name="hive_metastore")

    mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    src_external_table = make_table(schema_name=src_schema.name, external_csv=mounted_location)

    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    logger.info(f"dst_catalog={dst_catalog.name}, external_table={src_external_table.full_name}")

    # crawler = TablesCrawler(sql_backend, inventory_schema)
    crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_external_table])
    tm = TablesMigrate(crawler, ws, sql_backend, dst_catalog.name)
    tm.migrate_tables()

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_revert_migrated_table(ws, sql_backend, inventory_schema, make_schema, make_table):
    schema1 = make_schema(catalog_name="hive_metastore")
    schema2 = make_schema(catalog_name="hive_metastore")
    tables = [
        make_table(schema_name=schema1.name),
        make_table(schema_name=schema1.name),
        make_table(schema_name=schema2.name),
        make_table(schema_name=schema2.name),
    ]

    for i in range(3):
        logger.info(f"Applying upgraded_to table property for {tables[i].full_name}")
        sql_backend.execute(f"ALTER TABLE {tables[i].full_name} set TBLPROPERTIES('upgraded_to'='FAKE DEST')")

    crawler = TablesCrawler(sql_backend, inventory_schema)
    tm = TablesMigrate(crawler, ws, sql_backend)
    tm.revert_migrated_tables(schema=schema1.name)

    def checks_upgraded_to(table_name: str):
        result = sql_backend.fetch(f"SHOW TBLPROPERTIES {table_name}")
        for value in result:
            if value["key"] == "upgraded_to":
                logger.info(f"{table_name} is set as upgraded")
                return True
        logger.info(f"{table_name} is set as not upgraded")
        return False

    # Checking that two of the tables were reverted and one was left intact.
    assert [False,False,True,False] == [checks_upgraded_to(table.full_name) for table in tables]

    assessed_tables = sql_backend.fetch(f"SELECT database, name, upgraded_to from {inventory_schema}.tables "
                                        f"where database in ('{schema1.name}','{schema2.name}')")
    assessed_tables_list = list(assessed_tables)
    assert Row((tables[0].schema_name, tables[0].name, None)) in assessed_tables_list
    assert Row((tables[1].schema_name, tables[1].name, None)) in assessed_tables_list
    assert Row((tables[2].schema_name, tables[2].name, "fake dest")) in assessed_tables_list
    assert Row((tables[3].schema_name, tables[3].name, None)) in assessed_tables_list
    assert True

