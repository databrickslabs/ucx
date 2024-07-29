import logging
from datetime import timedelta

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import DataSecurityMode, AwsAttributes
from databricks.sdk.service.catalog import Privilege, SecurableType, TableInfo, TableType
from databricks.sdk.service.iam import PermissionLevel
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableMapping
from databricks.labs.ucx.hive_metastore.tables import AclMigrationWhat, Table, What

from ..conftest import prepare_hiveserde_tables, get_azure_spark_conf

logger = logging.getLogger(__name__)
_SPARK_CONF = get_azure_spark_conf()


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_managed_tables(ws, sql_backend, runtime_ctx, make_catalog):
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_managed_table = runtime_ctx.make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)

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
        catalog_name=src_schema.catalog_name, non_delta=True, schema_name=src_schema.name
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
    src_external_table = runtime_ctx.make_table(schema_name=src_schema.name, external_csv=make_mounted_location)
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
def test_migrate_external_table_failed_sync(ws, caplog, runtime_ctx, env_or_skip):
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
    runtime_ctx.tables_migrator.migrate_tables(what=What.EXTERNAL_SYNC)

    assert "SYNC command failed to migrate" in caplog.text


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_external_table_hiveserde_in_place(
    ws, sql_backend, make_random, runtime_ctx, make_storage_dir, env_or_skip, make_catalog
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

    runtime_ctx.tables_migrator.migrate_tables(
        what=What.EXTERNAL_HIVESERDE, mounts_crawler=runtime_ctx.mounts_crawler, hiveserde_in_place_migrate=True
    )

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
    ws, sql_backend, make_random, runtime_ctx, make_storage_dir, env_or_skip, make_catalog
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

    runtime_ctx.tables_migrator.migrate_tables(
        what=What.EXTERNAL_HIVESERDE, mounts_crawler=runtime_ctx.mounts_crawler, hiveserde_in_place_migrate=False
    )

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
    assert view1_view_text == f"SELECT * FROM {dst_schema.full_name}.{src_managed_table.name}"
    view2_view_text = ws.tables.get(f"{dst_schema.full_name}.{src_view2.name}").view_definition
    assert view2_view_text == f"SELECT * FROM {dst_schema.full_name}.{src_view1.name}"
    view3_view_text = next(iter(sql_backend.fetch(f"SHOW CREATE TABLE {dst_schema.full_name}.view3")))["createtab_stmt"]
    assert "(col1,col2)" in view3_view_text.replace("\n", "").replace(" ", "").lower()


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
    src_schema1 = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_schema2 = runtime_ctx.make_schema(catalog_name="hive_metastore")
    table_to_migrate = runtime_ctx.make_table(schema_name=src_schema1.name)
    table_databricks_dataset = runtime_ctx.make_table(
        schema_name=src_schema1.name, external_csv="dbfs:/databricks-datasets/adult/adult.data"
    )
    table_to_skip = runtime_ctx.make_table(schema_name=src_schema1.name)
    table_in_skipped_database = runtime_ctx.make_table(schema_name=src_schema2.name)

    dst_catalog = make_catalog()
    dst_schema1 = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema1.name)
    dst_schema2 = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema2.name)

    rules = [
        Rule.from_src_dst(table_to_migrate, dst_schema1),
        Rule.from_src_dst(table_to_skip, dst_schema1),
        Rule.from_src_dst(table_databricks_dataset, dst_schema1),
        Rule.from_src_dst(table_in_skipped_database, dst_schema2),
    ]
    runtime_ctx.with_table_mapping_rules(rules)
    table_mapping = runtime_ctx.table_mapping
    table_mapping.skip_table(src_schema1.name, table_to_skip.name)
    table_mapping.skip_schema(src_schema2.name)
    assert len(table_mapping.get_tables_to_migrate(runtime_ctx.tables_crawler)) == 1


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


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_migrate_managed_tables_with_acl(ws, sql_backend, runtime_ctx, make_catalog, make_user):
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_managed_table = runtime_ctx.make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)
    user = make_user()

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

    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA, acl_strategy=[AclMigrationWhat.LEGACY_TACL])

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_managed_table.name}").properties
    target_table_grants = ws.grants.get(SecurableType.TABLE, f"{dst_schema.full_name}.{src_managed_table.name}")
    assert target_table_properties["upgraded_from"] == src_managed_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())
    assert target_table_grants.privilege_assignments[0].principal == user.user_name
    assert target_table_grants.privilege_assignments[0].privileges == [Privilege.MODIFY, Privilege.SELECT]


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_migrate_external_tables_with_principal_acl_azure(
    ws, make_user, prepared_principal_acl, make_cluster_permissions, make_cluster, make_ucx_group
):
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
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC, acl_strategy=[AclMigrationWhat.PRINCIPAL])

    target_table_grants = ws.grants.get(SecurableType.TABLE, table_full_name)
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
    ws, make_user, prepared_principal_acl, make_cluster_permissions, make_cluster, env_or_skip
):
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
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC, acl_strategy=[AclMigrationWhat.PRINCIPAL])

    target_table_grants = ws.grants.get(SecurableType.TABLE, table_full_name)
    match = False
    for _ in target_table_grants.privilege_assignments:
        if _.principal == user.user_name and _.privileges == [Privilege.ALL_PRIVILEGES]:
            match = True
            break
    assert match


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_migrate_external_tables_with_principal_acl_aws_warehouse(
    ws, make_user, prepared_principal_acl, make_warehouse_permissions, make_warehouse, env_or_skip
):
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
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC, acl_strategy=[AclMigrationWhat.PRINCIPAL])

    target_table_grants = ws.grants.get(SecurableType.TABLE, table_full_name)
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
    make_acc_group,
):
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")
    config = WorkspaceConfig(
        warehouse_id=env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"),
        inventory_database=inventory_schema,
        connect=ws.config,
    )
    runtime_ctx = runtime_ctx.replace(config=config)
    tbl_path = make_random(4).lower()
    src_external_table = runtime_ctx.make_table(
        schema_name=make_schema(catalog_name="hive_metastore", name=f'mounted_{env_or_skip("TEST_MOUNT_NAME")}').name,
        external_delta=f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/{tbl_path}',
    )
    table_in_mount_location = f"abfss://things@labsazurethings.dfs.core.windows.net/a/b/{tbl_path}"
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
                f'mounted_{env_or_skip("TEST_MOUNT_NAME")}',
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
    ws, make_user, prepared_principal_acl, make_cluster_permissions, make_cluster
):
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
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC, acl_strategy=[AclMigrationWhat.PRINCIPAL])

    target_table_grants = ws.grants.get(SecurableType.TABLE, table_full_name)
    match = False
    for _ in target_table_grants.privilege_assignments:
        if _.principal == spn_with_mount_access and _.privileges == [Privilege.ALL_PRIVILEGES]:
            match = True
            break
    assert match


def test_migrate_table_uppercase_column_names(ws,
                                              sql_backend,
                                              runtime_ctx,
                                              make_catalog,
                                              make_mounted_location,
                                              make_random):
    table_name = f"ucx_{make_random(4)}".lower()
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    storage_override = f'dbfs:/user/hive/warehouse/{src_schema.name}/{table_name}'
    table_ddl = f"CREATE TABLE hive_metastore.{src_schema.name}.{table_name} (ID int, VALUE string)"
    src_table = runtime_ctx.make_table(schema_name=src_schema.name,
                                       name=table_name,
                                       hiveserde_ddl=table_ddl,
                                       storage_override=storage_override)
    dst_catalog = make_catalog()
    dst_schema = runtime_ctx.make_schema(catalog_name=dst_catalog.name, name=src_schema.name)
    logger.info(f"dst_catalog={dst_catalog.name}, external_table={src_table.full_name}")
    rules = [Rule.from_src_dst(src_table, dst_schema)]

    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()
    runtime_ctx.tables_migrator.migrate_tables(what=What.DBFS_ROOT_DELTA)

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
    target_table = ws.tables.get(f"{dst_schema.full_name}.{src_table.name}")
    target_table_properties = target_table.properties
    assert target_table_properties["upgraded_from"] == src_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())

    migration_status = list(runtime_ctx.migration_status_refresher.snapshot())
    assert len(migration_status) == 1
    assert migration_status[0].src_schema == src_table.schema_name
    assert migration_status[0].src_table == src_table.name
    assert migration_status[0].dst_catalog == dst_catalog.name
    assert migration_status[0].dst_schema == dst_schema.name
    assert migration_status[0].dst_table == src_table.name

    assert target_table.columns[0].name == "ID"
    assert target_table.columns[1].name == "VALUE"
