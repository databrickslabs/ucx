import logging
from datetime import timedelta

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import Privilege, PrivilegeAssignment, SecurableType

from databricks.labs.ucx.hive_metastore.mapping import Rule
from databricks.labs.ucx.hive_metastore.table_migrate import TablesMigrate
from databricks.labs.ucx.framework.parallel import ManyError
from databricks.labs.ucx.hive_metastore.tables import TablesMigrate

from ..conftest import StaticTableMapping, StaticTablesCrawler

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

    table_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_managed_table])
    rules = [
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema.name,
            dst_schema.name,
            src_managed_table.name,
            src_managed_table.name,
        ),
    ]
    table_mapping = StaticTableMapping(rules=rules)
    table_migrate = TablesMigrate(table_crawler, ws, sql_backend, table_mapping)

    table_migrate.migrate_tables()

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
    table_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_managed_table])
    rules = [
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema.name,
            dst_schema.name,
            src_managed_table.name,
            dst_managed_table.name,
        ),
    ]
    table_mapping = StaticTableMapping(rules=rules)
    table_migrate = TablesMigrate(table_crawler, ws, sql_backend, table_mapping)

    # FIXME: flaky: databricks.sdk.errors.mapping.NotFound: Catalog 'ucx_cjazg' does not exist.
    table_migrate.migrate_tables()

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
    table_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_external_table])
    rules = [
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema.name,
            dst_schema.name,
            src_external_table.name,
            src_external_table.name,
        ),
    ]
    table_mapping = StaticTableMapping(rules=rules)
    table_migrate = TablesMigrate(table_crawler, ws, sql_backend, table_mapping)

    table_migrate.migrate_tables()

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_revert_migrated_table(ws, sql_backend, inventory_schema, make_schema, make_table, make_catalog):
    src_schema1 = make_schema(catalog_name="hive_metastore")
    src_schema2 = make_schema(catalog_name="hive_metastore")
    table_to_revert = make_table(schema_name=src_schema1.name)
    table_not_migrated = make_table(schema_name=src_schema1.name)
    table_to_not_revert = make_table(schema_name=src_schema2.name)
    all_tables = [table_to_revert, table_not_migrated, table_to_not_revert]

    dst_catalog = make_catalog()
    dst_schema1 = make_schema(catalog_name=dst_catalog.name, name=src_schema1.name)
    dst_schema2 = make_schema(catalog_name=dst_catalog.name, name=src_schema2.name)

    table_crawler = StaticTablesCrawler(sql_backend, inventory_schema, all_tables)
    rules = [
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema1.name,
            dst_schema1.name,
            table_to_revert.name,
            table_to_revert.name,
        ),
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema2.name,
            dst_schema2.name,
            table_to_not_revert.name,
            table_to_not_revert.name,
        ),
    ]
    table_mapping = StaticTableMapping(rules=rules)
    table_migrate = TablesMigrate(table_crawler, ws, sql_backend, table_mapping)
    table_migrate.migrate_tables()

    table_migrate.revert_migrated_tables(src_schema1.name, delete_managed=True)

    # Checking that two of the tables were reverted and one was left intact.
    # The first two table belongs to schema 1 and should have not "upgraded_to" property
    assert not table_migrate.is_upgraded(table_to_revert.schema_name, table_to_revert.name)
    # The second table didn't have the "upgraded_to" property set and should remain that way.
    assert not table_migrate.is_upgraded(table_not_migrated.schema_name, table_not_migrated.name)
    # The third table belongs to schema2 and had the "upgraded_to" property set and should remain that way.
    assert table_migrate.is_upgraded(table_to_not_revert.schema_name, table_to_not_revert.name)

    target_tables_schema1 = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema1.full_name}"))
    assert len(target_tables_schema1) == 0

    target_tables_schema2 = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema2.full_name}"))
    assert len(target_tables_schema2) == 1
    assert target_tables_schema2[0]["database"] == dst_schema2.name
    assert target_tables_schema2[0]["tableName"] == table_to_not_revert.name


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_uc_to_uc_no_from_schema(ws, sql_backend, inventory_schema):
    static_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [])
    tm = TablesMigrate(static_crawler, ws, sql_backend)
    with pytest.raises(ManyError):
        tm.migrate_uc_tables(
            from_catalog="SrcC", from_schema="SrcS", from_table=["*"], to_catalog="TgtC", to_schema="TgtS"
        )


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_uc_to_uc(ws, sql_backend, inventory_schema, make_catalog, make_schema, make_table, make_acc_group):
    static_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [])
    tm = TablesMigrate(static_crawler, ws, sql_backend)
    group_a = make_acc_group()
    group_b = make_acc_group()
    from_catalog = make_catalog()
    from_schema = make_schema(catalog_name=from_catalog.name)
    from_table_1 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_table_2 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_table_3 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_view_1 = make_table(
        catalog_name=from_catalog.name,
        schema_name=from_schema.name,
        view=True,
        ctas=f"select * from {from_table_2.full_name}",
    )
    to_catalog = make_catalog()
    to_schema = make_schema(catalog_name=to_catalog.name)
    # creating a table in target schema to test skipping
    to_table_3 = make_table(catalog_name=to_catalog.name, schema_name=to_schema.name, name=from_table_3.name)
    sql_backend.execute(f"GRANT SELECT ON TABLE {from_table_1.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT SELECT,MODIFY ON TABLE {from_table_2.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON VIEW {from_view_1.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {to_table_3.full_name} TO `{group_a.display_name}`")
    tm.migrate_uc_tables(
        from_catalog=from_catalog.name,
        from_schema=from_schema.name,
        from_table=["*"],
        to_catalog=to_catalog.name,
        to_schema=to_schema.name,
    )
    tables = ws.tables.list(catalog_name=to_catalog.name, schema_name=to_schema.name)
    table_1_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_table_1.name}"
    )
    table_2_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_table_2.name}"
    )
    table_3_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_table_3.name}"
    )
    view_1_grant = ws.grants.get(
        securable_type=SecurableType.TABLE, full_name=f"{to_catalog.name}.{to_schema.name}.{from_view_1.name}"
    )
    assert (
        len(
            [t for t in tables if t.name in [from_table_1.name, from_table_2.name, from_table_3.name, from_view_1.name]]
        )
        == 4
    )
    expected_table_1_grant = [PrivilegeAssignment(group_a.display_name, [Privilege.SELECT])]
    expected_table_2_grant = [
        PrivilegeAssignment(group_b.display_name, [Privilege.MODIFY, Privilege.SELECT]),
    ]
    expected_table_3_grant = [PrivilegeAssignment(group_a.display_name, [Privilege.SELECT])]
    expected_view_1_grant = [PrivilegeAssignment(group_b.display_name, [Privilege.SELECT])]

    assert len([perm for perm in table_1_grant.privilege_assignments if perm in expected_table_1_grant]) == 1
    assert len([perm for perm in table_2_grant.privilege_assignments if perm in expected_table_2_grant]) == 1
    assert len([perm for perm in table_3_grant.privilege_assignments if perm in expected_table_3_grant]) == 1
    assert len([perm for perm in view_1_grant.privilege_assignments if perm in expected_view_1_grant]) == 1


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_uc_to_uc_no_to_schema(ws, sql_backend, inventory_schema, make_catalog, make_schema, make_table, make_random):
    static_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [])
    tm = TablesMigrate(static_crawler, ws, sql_backend)
    from_catalog = make_catalog()
    from_schema = make_schema(catalog_name=from_catalog.name)
    from_table_1 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_table_2 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    from_table_3 = make_table(catalog_name=from_catalog.name, schema_name=from_schema.name)
    to_catalog = make_catalog()
    to_schema = make_random(4)
    tm.migrate_uc_tables(
        from_catalog=from_catalog.name,
        from_schema=from_schema.name,
        from_table=[from_table_1.name, from_table_2.name],
        to_catalog=to_catalog.name,
        to_schema=to_schema,
    )
    tables = ws.tables.list(catalog_name=to_catalog.name, schema_name=to_schema)
    assert len([t for t in tables if t.name in [from_table_1.name, from_table_2.name, from_table_3.name]]) == 2
