import logging
from datetime import timedelta

import pytest
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import DataSecurityMode, AwsAttributes
from databricks.sdk.service.catalog import Privilege, SecurableType, TableInfo, TableType
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.__about__ import __version__

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableMapping
from databricks.labs.ucx.hive_metastore.tables import Table, What

from ..conftest import prepare_hiveserde_tables, get_azure_spark_conf


logger = logging.getLogger(__name__)
_SPARK_CONF = get_azure_spark_conf()


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_managed_tables(ws, sql_backend, runtime_ctx, make_catalog):
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_managed_table = runtime_ctx.make_table(
        catalog_name=src_schema.catalog_name,
        schema_name=src_schema.name,
        columns=[("-das-hes-", "STRING")],  # Test with column that needs escaping
    )

    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    logger.info(f"dst_catalog={dst_catalog.name}, managed_table={src_managed_table.full_name}")

    rules = [Rule.from_src_dst(src_managed_table, dst_schema)]

    runtime_ctx.with_table_mapping_rules(rules)

    runtime_ctx.with_dummy_resource_permission()
    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA)

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_managed_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_managed_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_dbfs_non_delta_tables(ws, sql_backend, runtime_ctx, make_catalog):
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_managed_table = runtime_ctx.make_table(
        catalog_name=src_schema.catalog_name,
        non_delta=True,
        schema_name=src_schema.name,
        columns=[("1-0`.0-ugly-column", "STRING")],  # Test with column that needs escaping
    )

    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    logger.info(f"dst_catalog={dst_catalog.name}, managed_table={src_managed_table.full_name}")

    rules = [Rule.from_src_dst(src_managed_table, dst_schema)]

    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()
    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_NON_DELTA)

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_managed_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_managed_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_tables_with_cache_should_not_create_table(
    ws,
    sql_backend,
    runtime_ctx,
    make_random,
    make_catalog,
):
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")

    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    table_name = make_random().lower()
    src_managed_table = runtime_ctx.make_table(
        catalog_name=src_schema.catalog_name,
        schema_name=src_schema.name,
        name=table_name,
        tbl_properties={"upgraded_from": f"{dst_schema.full_name}.{table_name}"},
    )
    dst_managed_table = runtime_ctx.make_table(
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
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()

    # FIXME: flaky: databricks.sdk.errors.platform.NotFound: Catalog 'ucx_cjazg' does not exist.
    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA)

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
    assert target_tables[0]["database"] == dst_schema.name
    assert target_tables[0]["tableName"] == table_name


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_external_table(
    ws,
    sql_backend,
    runtime_ctx,
    make_catalog,
    make_mounted_location,
):
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_external_table = runtime_ctx.make_table(
        schema_name=src_schema.name,
        external_csv=make_mounted_location,
        columns=[("`back`ticks`", "STRING")],  # Test with column that needs escaping
    )
    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)
    logger.info(f"dst_catalog={dst_catalog.name}, external_table={src_external_table.full_name}")
    rules = [Rule.from_src_dst(src_external_table, dst_schema)]

    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()
    runtime_ctx.tables_migrator.migrate_tables(what=What.EXTERNAL_SYNC)

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_external_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_external_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())

    migration_status = list(runtime_ctx.migration_status_refresher.snapshot())
    assert len(migration_status) == 1
    assert migration_status[0].src_schema == src_external_table.schema_name
    assert migration_status[0].src_table == src_external_table.name
    assert migration_status[0].dst_catalog == dst_catalog.name
    assert migration_status[0].dst_schema == dst_schema.name
    assert migration_status[0].dst_table == src_external_table.name


def test_migrate_managed_table_to_external_table_without_conversion(
    ws,
    sql_backend,
    runtime_ctx,
    make_catalog,
    make_mounted_location,
    make_random,
    env_or_skip,
):
    src_schema_name = f"dummy_s{make_random(4)}".lower()
    src_schema_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/{src_schema_name}'
    src_schema = runtime_ctx.make_schema(name=src_schema_name, location=src_schema_location)
    src_external_table = runtime_ctx.make_table(
        schema_name=src_schema.name,
        external=False,
        columns=[("`back`ticks`", "STRING")],  # Test with column that needs escaping
    )
    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)
    logger.info(f"dst_catalog={dst_catalog.name}, external_table={src_external_table.full_name}")
    rules = [Rule.from_src_dst(src_external_table, dst_schema)]

    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()
    runtime_ctx.tables_migrator.migrate_tables(
        what=What.EXTERNAL_SYNC, managed_table_external_storage="SYNC_AS_EXTERNAL"
    )

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_external_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_external_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())

    migration_status = list(runtime_ctx.migration_status_refresher.snapshot())
    assert len(migration_status) == 1
    assert migration_status[0].src_schema == src_external_table.schema_name
    assert migration_status[0].src_table == src_external_table.name
    assert migration_status[0].dst_catalog == dst_catalog.name
    assert migration_status[0].dst_schema == dst_schema.name
    assert migration_status[0].dst_table == src_external_table.name


def test_migrate_managed_table_to_external_table_with_clone(
    ws,
    sql_backend,
    runtime_ctx,
    make_catalog,
    make_mounted_location,
    make_random,
    env_or_skip,
):
    src_schema_name = f"dummy_s{make_random(4)}".lower()
    src_schema_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/{src_schema_name}'
    src_schema = runtime_ctx.make_schema(name=src_schema_name, location=src_schema_location)
    src_external_table = runtime_ctx.make_table(
        schema_name=src_schema.name,
        external=False,
        columns=[("`back`ticks`", "STRING")],  # Test with column that needs escaping
    )
    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)
    logger.info(f"dst_catalog={dst_catalog.name}, external_table={src_external_table.full_name}")
    rules = [Rule.from_src_dst(src_external_table, dst_schema)]

    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()
    runtime_ctx.tables_migrator.migrate_tables(what=What.EXTERNAL_SYNC)

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_external_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_external_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())

    migration_status = list(runtime_ctx.migration_status_refresher.snapshot())
    assert len(migration_status) == 1
    assert migration_status[0].src_schema == src_external_table.schema_name
    assert migration_status[0].src_table == src_external_table.name
    assert migration_status[0].dst_catalog == dst_catalog.name
    assert migration_status[0].dst_schema == dst_schema.name
    assert migration_status[0].dst_table == src_external_table.name


@retried(on=[NotFound], timeout=timedelta(minutes=1))
def test_migrate_external_table_failed_sync(caplog, runtime_ctx, env_or_skip) -> None:
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    existing_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    src_external_table = runtime_ctx.make_table(schema_name=src_schema.name, external_csv=existing_mounted_location)
    # create a mapping that will fail the SYNC because the target catalog and schema does not exist
    rules = [
        Rule(
            "workspace",
            "non_existed_catalog",
            src_schema.name,
            "existed_schema",
            src_external_table.name,
            src_external_table.name,
        ),
    ]
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.hive_metastore.table_migrate"):
        runtime_ctx.tables_migrator.migrate_tables(what=What.EXTERNAL_SYNC)
    assert "SYNC command failed to migrate" in caplog.text


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_external_table_hiveserde_in_place(
    ws,
    sql_backend,
    make_random,
    runtime_ctx,
    make_storage_dir,
    env_or_skip,
    make_catalog,
):
    random = make_random(4).lower()
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore", name=f"hiveserde_in_place_{random}")
    # prepare source external table location
    table_base_dir = make_storage_dir(path=f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/hiveserde_in_place_{random}')
    # prepare source tables
    src_tables = prepare_hiveserde_tables(runtime_ctx, random, src_schema, table_base_dir)
    for src_table in src_tables.values():
        sql_backend.execute(f"INSERT INTO {src_table.full_name} VALUES (1, 'us')")
    # prepare target catalog and schema
    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    rules = [Rule.from_src_dst(table, dst_schema) for _, table in src_tables.items()]
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()

    runtime_ctx.tables_migrator.migrate_tables(what=What.EXTERNAL_HIVESERDE, hiveserde_in_place_migrate=True)

    # assert results
    for src_table in src_tables.values():
        try:
            target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_table.name}").properties
            row = next(sql_backend.fetch(f"SELECT * FROM {dst_schema.full_name}.{src_table.name}"))
        except NotFound:
            assert False, f"{src_table.name} not found in {dst_schema.full_name}"
        assert target_table_properties["upgraded_from"] == src_table.full_name
        assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())
        assert row["id"] == 1
        assert row["region"] == "us"


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_external_table_hiveserde_ctas(
    ws,
    sql_backend,
    make_random,
    runtime_ctx,
    make_storage_dir,
    env_or_skip,
    make_catalog,
):
    random = make_random(4).lower()
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore", name=f"hiveserde_ctas_{random}")
    # prepare source external table location
    table_base_dir = make_storage_dir(path=f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/hiveserde_ctas_{random}')
    # prepare source tables
    src_tables = prepare_hiveserde_tables(runtime_ctx, random, src_schema, table_base_dir)
    for src_table in src_tables.values():
        sql_backend.execute(f"INSERT INTO {src_table.full_name} VALUES (1, 'us')")
    # prepare target catalog and schema
    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    rules = [Rule.from_src_dst(table, dst_schema) for _, table in src_tables.items()]
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()

    runtime_ctx.tables_migrator.migrate_tables(what=What.EXTERNAL_HIVESERDE, hiveserde_in_place_migrate=False)

    # assert results
    for src_table in src_tables.values():
        try:
            target_table = ws.tables.get(f"{dst_schema.full_name}.{src_table.name}")
            row = next(sql_backend.fetch(f"SELECT * FROM {dst_schema.full_name}.{src_table.name}"))
        except NotFound:
            assert False, f"{src_table.name} not found in {dst_schema.full_name}"
        assert target_table.properties["upgraded_from"] == src_table.full_name
        assert target_table.properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())
        assert target_table.storage_location.endswith("_ctas_migrated")
        assert row["id"] == 1
        assert row["region"] == "us"


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_migrate_view(ws, sql_backend, runtime_ctx, make_catalog):
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_managed_table = runtime_ctx.make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)
    src_view1 = runtime_ctx.make_table(
        catalog_name=src_schema.catalog_name,
        schema_name=src_schema.name,
        ctas=f"SELECT * FROM {src_managed_table.full_name}",
        view=True,
    )
    src_view2 = runtime_ctx.make_table(
        catalog_name=src_schema.catalog_name,
        schema_name=src_schema.name,
        ctas=f"SELECT * FROM {src_view1.full_name}",
        view=True,
    )

    sql_backend.execute(
        f"CREATE VIEW {src_schema.full_name}.view3 (col1,col2) as " f"SELECT * FROM {src_managed_table.full_name}"
    )
    runtime_ctx.add_table(
        TableInfo(
            catalog_name=src_schema.catalog_name,
            schema_name=src_schema.name,
            name="view3",
            table_type=TableType.VIEW,
            full_name=f"{src_schema.full_name}.view3",
            view_definition=f"SELECT * FROM {src_managed_table.full_name}",
        )
    )

    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    logger.info(f"dst_catalog={dst_catalog.name}, managed_table={src_managed_table.full_name}")

    rules = [
        Rule.from_src_dst(src_managed_table, dst_schema),
        Rule.from_src_dst(src_view1, dst_schema),
        Rule.from_src_dst(src_view2, dst_schema),
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema.name,
            dst_schema.name,
            "view3",
            "view3",
        ),
    ]

    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()
    runtime_ctx.tables_migrator.index()
    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA)
    runtime_ctx.migration_status_refresher.snapshot()
    runtime_ctx.tables_migrator.migrate_tables(what=What.VIEW)
    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 4

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_managed_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_managed_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())
    view1_view_text = ws.tables.get(f"{dst_schema.full_name}.{src_view1.name}").view_definition
    assert (
        view1_view_text == f"SELECT * FROM `{dst_schema.catalog_name}`.`{dst_schema.name}`.`{src_managed_table.name}`"
    )
    view2_view_text = ws.tables.get(f"{dst_schema.full_name}.{src_view2.name}").view_definition
    assert view2_view_text == f"SELECT * FROM `{dst_schema.catalog_name}`.`{dst_schema.name}`.`{src_view1.name}`"
    view3_view_text = next(iter(sql_backend.fetch(f"SHOW CREATE TABLE {dst_schema.full_name}.view3")))["createtab_stmt"]
    assert "(col1,col2)" in view3_view_text.replace("\n", "").replace(" ", "").lower()


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_migrate_view_alias_test(ws, sql_backend, runtime_ctx, make_catalog):
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_managed_table = runtime_ctx.make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)
    src_view1 = runtime_ctx.make_table(
        catalog_name=src_schema.catalog_name,
        schema_name=src_schema.name,
        ctas=f"SELECT A.* FROM {src_managed_table.full_name} AS A",
        view=True,
    )

    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    logger.info(f"dst_catalog={dst_catalog.name}, managed_table={src_managed_table.full_name}")

    rules = [
        Rule.from_src_dst(src_managed_table, dst_schema),
        Rule.from_src_dst(src_view1, dst_schema),
    ]

    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()
    runtime_ctx.tables_migrator.index()
    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA)
    runtime_ctx.migration_status_refresher.snapshot()
    runtime_ctx.tables_migrator.migrate_tables(what=What.VIEW)
    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 2

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_managed_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_managed_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())
    view1_view_text = ws.tables.get(f"{dst_schema.full_name}.{src_view1.name}").view_definition
    assert (
        view1_view_text
        == f"SELECT `A`.* FROM `{dst_schema.catalog_name}`.`{dst_schema.name}`.`{src_managed_table.name}` AS `A`"
    )


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_revert_migrated_table(sql_backend, runtime_ctx, make_catalog):
    src_schema1 = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_schema2 = runtime_ctx.make_schema(catalog_name="hive_metastore")
    table_to_revert = runtime_ctx.make_table(schema_name=src_schema1.name)
    table_not_migrated = runtime_ctx.make_table(schema_name=src_schema1.name)
    table_to_not_revert = runtime_ctx.make_table(schema_name=src_schema2.name)

    dst_catalog = make_catalog()
    dst_schema1 = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema1.name)
    dst_schema2 = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema2.name)

    rules = [
        Rule.from_src_dst(table_to_revert, dst_schema1),
        Rule.from_src_dst(table_to_not_revert, dst_schema2),
    ]
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()

    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA)

    runtime_ctx.tables_migrator.revert_migrated_tables(src_schema1.name, delete_managed=True)

    # Checking that two of the tables were reverted and one was left intact.
    # The first two table belongs to schema 1 and should have not "upgraded_to" property
    assert not runtime_ctx.tables_migrator.is_migrated(table_to_revert.schema_name, table_to_revert.name)
    # The second table didn't have the "upgraded_to" property set and should remain that way.
    assert not runtime_ctx.tables_migrator.is_migrated(table_not_migrated.schema_name, table_not_migrated.name)
    # The third table belongs to schema2 and had the "upgraded_to" property set and should remain that way.
    assert runtime_ctx.tables_migrator.is_migrated(table_to_not_revert.schema_name, table_to_not_revert.name)

    target_tables_schema1 = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema1.full_name}"))
    assert len(target_tables_schema1) == 0

    target_tables_schema2 = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema2.full_name}"))
    assert len(target_tables_schema2) == 1
    assert target_tables_schema2[0]["database"] == dst_schema2.name
    assert target_tables_schema2[0]["tableName"] == table_to_not_revert.name


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_mapping_skips_tables_databases(ws, sql_backend, runtime_ctx, make_catalog):
    # using lists to avoid MyPi 'too-many-variables' error
    src_schemas = [
        runtime_ctx.make_schema(catalog_name="hive_metastore"),
        runtime_ctx.make_schema(catalog_name="hive_metastore"),
    ]
    src_tables = [
        runtime_ctx.make_table(schema_name=src_schemas[0].name),  # table to migrate
        runtime_ctx.make_table(
            schema_name=src_schemas[0].name, external_csv="dbfs:/databricks-datasets/adult/adult.data"
        ),  # table databricks dataset
        runtime_ctx.make_table(schema_name=src_schemas[0].name),  # table to skip
        runtime_ctx.make_table(schema_name=src_schemas[1].name),  # table in skipped database
    ]
    src_tables.extend(
        [
            runtime_ctx.make_table(
                schema_name=src_schemas[0].name,
                ctas=f"SELECT * FROM {src_tables[0].full_name}",
                view=True,
            ),  # view to migrate
            runtime_ctx.make_table(
                schema_name=src_schemas[0].name,
                ctas=f"SELECT * FROM {src_tables[2].full_name}",
                view=True,
            ),  # view to skip
            runtime_ctx.make_table(
                schema_name=src_schemas[1].name,
                ctas=f"SELECT * FROM {src_tables[3].full_name}",
                view=True,
            ),  # view in schema to skip
        ]
    )

    dst_catalog = make_catalog()
    dst_schemas = []
    for src_schema in src_schemas:
        dst_schemas.append(runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name))

    rules = [
        Rule.from_src_dst(src_tables[0], dst_schemas[0]),
        Rule.from_src_dst(src_tables[1], dst_schemas[0]),
        Rule.from_src_dst(src_tables[2], dst_schemas[0]),
        Rule.from_src_dst(src_tables[3], dst_schemas[1]),
        Rule.from_src_dst(src_tables[4], dst_schemas[0]),
        Rule.from_src_dst(src_tables[5], dst_schemas[0]),
        Rule.from_src_dst(src_tables[6], dst_schemas[1]),
    ]
    runtime_ctx.with_table_mapping_rules(rules)
    table_mapping = runtime_ctx.table_mapping
    table = Table(
        "hive_metastore",
        src_schemas[0].name,
        src_tables[2].name,
        object_type="UNKNOWN",
        table_format="UNKNOWN",
    )
    table_mapping.skip_table_or_view(src_schemas[0].name, src_tables[2].name, load_table=lambda *_: table)
    table = Table(
        "hive_metastore",
        src_schemas[0].name,
        src_tables[5].name,
        object_type="UNKNOWN",
        table_format="UNKNOWN",
        view_text="SELECT 1",
    )
    table_mapping.skip_table_or_view(src_schemas[0].name, src_tables[5].name, load_table=lambda *_: table)
    table_mapping.skip_schema(src_schemas[1].name)
    tables_to_migrate = table_mapping.get_tables_to_migrate(runtime_ctx.tables_crawler)
    full_names = set(tm.src.full_name for tm in tables_to_migrate)
    assert full_names == {src_tables[0].full_name, src_tables[4].full_name}


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_mapping_reverts_table(ws, sql_backend, runtime_ctx, make_catalog):
    # prepare 2 tables to migrate
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    table_to_revert = runtime_ctx.make_table(schema_name=src_schema.name)
    table_to_skip = runtime_ctx.make_table(schema_name=src_schema.name)

    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    runtime_ctx.with_dummy_resource_permission()

    # set up ucx to migrate only 1 table
    runtime_ctx.with_table_mapping_rules(
        [
            Rule.from_src_dst(table_to_skip, dst_schema),
        ]
    )
    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA)
    # validate that the table is migrated successfully
    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{table_to_skip.name}").properties
    assert target_table_properties["upgraded_from"] == table_to_skip.full_name

    # mock that the other table was migrated
    sql_backend.execute(
        f"ALTER TABLE {table_to_revert.full_name} SET "
        f"TBLPROPERTIES('upgraded_to' = 'fake_catalog.fake_schema.fake_table');"
    )

    results = {_["key"]: _["value"] for _ in list(sql_backend.fetch(f"SHOW TBLPROPERTIES {table_to_revert.full_name}"))}
    assert "upgraded_to" in results
    assert results["upgraded_to"] == "fake_catalog.fake_schema.fake_table"

    # set up ucx to migrate both tables
    runtime_ctx.with_table_mapping_rules(
        [
            Rule.from_src_dst(table_to_revert, dst_schema),
            Rule.from_src_dst(table_to_skip, dst_schema),
        ]
    )
    table_mapping = TableMapping(runtime_ctx.installation, ws, sql_backend)
    mapping = list(table_mapping.get_tables_to_migrate(runtime_ctx.tables_crawler))

    # Checking to validate that table_to_skip was omitted from the list of rules
    assert len(mapping) == 1
    assert mapping[0].rule == Rule(
        "workspace",
        dst_catalog.name,
        src_schema.name,
        dst_schema.name,
        table_to_revert.name,
        table_to_revert.name,
    )
    results = {_["key"]: _["value"] for _ in list(sql_backend.fetch(f"SHOW TBLPROPERTIES {table_to_revert.full_name}"))}
    assert "upgraded_to" not in results


# @retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_migrate_managed_tables_with_acl(
    ws: WorkspaceClient, sql_backend, runtime_ctx, make_catalog, make_user, env_or_skip
) -> None:
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_managed_table = runtime_ctx.make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)
    user = make_user()

    acc_client = AccountClient(
        host=ws.config.environment.deployment_url("accounts"),
        account_id=env_or_skip("DATABRICKS_ACCOUNT_ID"),
        product='ucx',
        product_version=__version__,
    )
    runtime_ctx.with_workspace_info([acc_client.workspaces.get(ws.get_workspace_id())])
    runtime_ctx.make_grant(
        principal=user.user_name,
        action_type="SELECT",
        table_info=src_managed_table,
    )
    runtime_ctx.make_grant(
        principal=user.user_name,
        action_type="MODIFY",
        table_info=src_managed_table,
    )

    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)
    logger.info(f"dst_catalog={dst_catalog.name}, managed_table={src_managed_table.full_name}")

    rules = [Rule.from_src_dst(src_managed_table, dst_schema)]
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()

    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA)

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_managed_table.name}").properties
    assert target_table_properties
    assert target_table_properties["upgraded_from"] == src_managed_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())

    target_table_grants = ws.grants.get(SecurableType.TABLE.value, f"{dst_schema.full_name}.{src_managed_table.name}")
    target_principals = [pa for pa in target_table_grants.privilege_assignments or [] if pa.principal == user.user_name]
    assert len(target_principals) == 1, f"Missing grant for user {user.user_name}"
    assert target_principals[0].privileges == [Privilege.MODIFY, Privilege.SELECT]

    acl_migrator = runtime_ctx.acl_migrator
    acls = acl_migrator.snapshot()
    assert (
        Grant(
            principal=user.user_name,
            action_type='MODIFY',
            catalog='hive_metastore',
            database=src_managed_table.schema_name,
            table=src_managed_table.name,
        )
        in acls
    )
    assert (
        Grant(
            principal=user.user_name,
            action_type="SELECT",
            catalog='hive_metastore',
            database=src_managed_table.schema_name,
            table=src_managed_table.name,
        )
        in acls
    )


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_migrate_external_tables_with_principal_acl_azure(
    ws: WorkspaceClient, make_user, prepared_principal_acl, make_cluster_permissions, make_cluster, make_ucx_group
) -> None:
    if not ws.config.is_azure:
        pytest.skip("only works in azure test env")
    ctx, table_full_name, _, _ = prepared_principal_acl
    cluster = make_cluster(single_node=True, spark_conf=_SPARK_CONF, data_security_mode=DataSecurityMode.NONE)
    ctx.with_dummy_resource_permission()
    table_migrate = ctx.tables_migrator

    user_with_cluster_access = make_user()
    user_without_cluster_access = make_user()
    group_with_cluster_access, _ = make_ucx_group()
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=PermissionLevel.CAN_ATTACH_TO,
        user_name=user_with_cluster_access.user_name,
        group_name=group_with_cluster_access.display_name,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    target_table_grants = ws.grants.get(SecurableType.TABLE.value, table_full_name)
    assert target_table_grants.privilege_assignments is not None
    match = False
    for _ in target_table_grants.privilege_assignments:
        if _.principal == user_with_cluster_access.user_name and _.privileges == [Privilege.ALL_PRIVILEGES]:
            match = True
            break
    assert match

    match = False
    for _ in target_table_grants.privilege_assignments:
        if _.principal == group_with_cluster_access.display_name and _.privileges == [Privilege.ALL_PRIVILEGES]:
            match = True
            break
    assert match

    for _ in target_table_grants.privilege_assignments:
        if _.principal == user_without_cluster_access.user_name and _.privileges == [Privilege.ALL_PRIVILEGES]:
            assert False, "User without cluster access should not have access to the table"
    assert True


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_migrate_external_tables_with_principal_acl_aws(
    ws: WorkspaceClient, make_user, prepared_principal_acl, make_cluster_permissions, make_cluster, env_or_skip
) -> None:
    ctx, table_full_name, _, _ = prepared_principal_acl
    ctx.with_dummy_resource_permission()
    cluster = make_cluster(
        single_node=True,
        data_security_mode=DataSecurityMode.NONE,
        aws_attributes=AwsAttributes(instance_profile_arn=env_or_skip("TEST_WILDCARD_INSTANCE_PROFILE")),
    )
    table_migrate = ctx.tables_migrator
    user = make_user()
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=PermissionLevel.CAN_ATTACH_TO,
        user_name=user.user_name,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    target_table_grants = ws.grants.get(SecurableType.TABLE.value, table_full_name)
    assert target_table_grants.privilege_assignments is not None
    match = False
    for _ in target_table_grants.privilege_assignments:
        if _.principal == user.user_name and _.privileges == [Privilege.ALL_PRIVILEGES]:
            match = True
            break
    assert match


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_migrate_external_tables_with_principal_acl_aws_warehouse(
    ws: WorkspaceClient,
    make_user,
    prepared_principal_acl,
    make_warehouse_permissions,
    make_warehouse,
) -> None:
    if not ws.config.is_aws:
        pytest.skip("temporary: only works in aws test env")
    ctx, table_full_name, _, _ = prepared_principal_acl
    ctx.with_dummy_resource_permission()
    warehouse = make_warehouse()
    table_migrate = ctx.tables_migrator
    user = make_user()
    make_warehouse_permissions(
        object_id=warehouse.id,
        permission_level=PermissionLevel.CAN_USE,
        user_name=user.user_name,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    target_table_grants = ws.grants.get(SecurableType.TABLE.value, table_full_name)
    assert target_table_grants.privilege_assignments is not None
    match = False
    for _ in target_table_grants.privilege_assignments:
        if _.principal == user.user_name and _.privileges == [Privilege.ALL_PRIVILEGES]:
            match = True
            break
    assert match


def test_migrate_table_in_mount(
    ws,
    sql_backend,
    inventory_schema,
    make_catalog,
    make_schema,
    env_or_skip,
    make_random,
    runtime_ctx,
):
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")
    config = WorkspaceConfig(
        warehouse_id=env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"),
        inventory_database=inventory_schema,
        connect=ws.config,
    )
    runtime_ctx = runtime_ctx.replace(config=config)
    b_dir = make_random(4).lower()
    table_path = make_random(4).lower()
    src_schema = make_schema(
        catalog_name="hive_metastore",
        name=f"mounted_{env_or_skip('TEST_MOUNT_NAME')}_{table_path}",
    )
    src_external_table = runtime_ctx.make_table(
        schema_name=src_schema.name,
        external_delta=f"dbfs:/mnt/{env_or_skip('TEST_MOUNT_NAME')}/a/{b_dir}/{table_path}",
        columns=[("1-0`.0-ugly-column", "STRING")],  # Test with column that needs escaping
    )
    table_in_mount_location = f"abfss://things@labsazurethings.dfs.core.windows.net/a/{b_dir}/{table_path}"
    # TODO: Remove this hack below
    # This is done because we have to create the external table in a mount point, but TablesInMounts() has to use the adls/ path
    # Otherwise, if we keep the dbfs:/ path, the entire logic of TablesInMounts won't work
    src_external_table.storage_location = table_in_mount_location
    runtime_ctx.save_tables()
    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name)

    runtime_ctx.with_table_mapping_rules(
        [
            Rule(
                "workspace",
                dst_catalog.name,
                src_schema.name,
                dst_schema.name,
                table_in_mount_location,
                src_external_table.name,
            ),
        ]
    )
    runtime_ctx.with_dummy_resource_permission()

    runtime_ctx.tables_migrator.migrate_tables(what=What.TABLE_IN_MOUNT)

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_external_table.name}")
    assert target_table_properties.properties["upgraded_from"] == table_in_mount_location


def test_migrate_external_tables_with_spn_azure(
    ws: WorkspaceClient, prepared_principal_acl, make_cluster_permissions, make_cluster
) -> None:
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")
    ctx, table_full_name, _, _ = prepared_principal_acl
    cluster = make_cluster(single_node=True, spark_conf=_SPARK_CONF, data_security_mode=DataSecurityMode.NONE)
    ctx.with_dummy_resource_permission()

    table_migrate = ctx.tables_migrator

    spn_with_mount_access = "5a11359f-ba1f-483f-8e00-0fe55ec003ed"
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=PermissionLevel.CAN_ATTACH_TO,
        service_principal_name=spn_with_mount_access,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    target_table_grants = ws.grants.get(SecurableType.TABLE.value, table_full_name)
    assert target_table_grants.privilege_assignments is not None
    match = False
    for _ in target_table_grants.privilege_assignments:
        if _.principal == spn_with_mount_access and _.privileges == [Privilege.ALL_PRIVILEGES]:
            match = True
            break
    assert match


def test_migration_index_deleted_source(make_table, runtime_ctx, sql_backend, make_catalog, make_schema, caplog):
    src_table = make_table()
    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name)
    # Create table in the destination schema
    sql_backend.execute(f"CREATE TABLE {dst_schema.full_name}.fake_table (id INT)")

    # Set the target table with non-existing source
    sql_backend.execute(
        f"ALTER TABLE {dst_schema.full_name}.fake_table SET "
        f"TBLPROPERTIES('upgraded_from' = '{src_table.full_name}');"
    )
    # Get the latest migration index
    tables = runtime_ctx.tables_crawler.snapshot()

    # drop the source table
    sql_backend.execute(f"DROP TABLE {src_table.full_name}")
    # Assert tables contains the source table
    assert src_table.full_name in [table.full_name for table in tables]

    migration_index = runtime_ctx.tables_migrator.index(force_refresh=True)
    assert migration_index
    # Assert that an error message was recorded containing a line with the text "which does no longer exist"
    expected_message = (
        f"failed-to-migrate: {src_table.schema_name}.{src_table.name} set as a source does no longer exist"
    )
    assert any(expected_message in record.message for record in caplog.records)
