import logging
from datetime import timedelta
from unittest.mock import create_autospec

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import Privilege, SecurableType

from databricks.labs.ucx.hive_metastore import GrantsCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.mapping import Rule
from databricks.labs.ucx.hive_metastore.table_migrate import (
    MigrationStatusRefresher,
    TablesMigrator,
)
from databricks.labs.ucx.hive_metastore.tables import AclMigrationWhat, Table
from databricks.labs.ucx.workspace_access.groups import GroupManager

from ..conftest import (
    StaticGrantsCrawler,
    StaticTableMapping,
    StaticTablesCrawler,
    StaticUdfsCrawler,
)

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_managed_tables(ws, sql_backend, inventory_schema, make_catalog, make_schema, make_table):
    # pylint: disable=too-many-locals
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")
    src_schema = make_schema(catalog_name="hive_metastore")
    src_managed_table = make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)

    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    logger.info(f"dst_catalog={dst_catalog.name}, managed_table={src_managed_table.full_name}")

    table_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_managed_table])
    udf_crawler = StaticUdfsCrawler(sql_backend, inventory_schema, [])
    grant_crawler = GrantsCrawler(table_crawler, udf_crawler)
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
    table_mapping = StaticTableMapping(ws, sql_backend, rules=rules)
    group_manager = GroupManager(sql_backend, ws, inventory_schema)
    migration_status_refresher = MigrationStatusRefresher(ws, sql_backend, inventory_schema, table_crawler)
    table_migrate = TablesMigrator(
        table_crawler, grant_crawler, ws, sql_backend, table_mapping, group_manager, migration_status_refresher
    )

    table_migrate.migrate_tables()

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_managed_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_managed_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_migrate_tables_with_cache_should_not_create_table(
    ws, sql_backend, inventory_schema, make_random, make_catalog, make_schema, make_table
):  # pylint: disable=too-many-locals
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

    table_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_managed_table])
    udf_crawler = StaticUdfsCrawler(sql_backend, inventory_schema, [])
    grant_crawler = GrantsCrawler(table_crawler, udf_crawler)
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
    table_mapping = StaticTableMapping(ws, sql_backend, rules=rules)
    group_manager = GroupManager(sql_backend, ws, inventory_schema)
    migration_status_refresher = MigrationStatusRefresher(ws, sql_backend, inventory_schema, table_crawler)
    table_migrate = TablesMigrator(
        table_crawler, grant_crawler, ws, sql_backend, table_mapping, group_manager, migration_status_refresher
    )

    # FIXME: flaky: databricks.sdk.errors.platform.NotFound: Catalog 'ucx_cjazg' does not exist.
    table_migrate.migrate_tables()

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
    assert target_tables[0]["database"] == dst_schema.name
    assert target_tables[0]["tableName"] == table_name


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_migrate_external_table(  # pylint: disable=too-many-locals
    ws,
    sql_backend,
    inventory_schema,
    make_catalog,
    make_schema,
    make_table,
    env_or_skip,
    make_random,
    make_dbfs_data_copy,
):
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")
    src_schema = make_schema(catalog_name="hive_metastore")
    # make a copy of src data to a new location to avoid overlapping UC table path that will fail other
    # external table migration tests
    existing_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    new_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/{make_random(4)}'
    make_dbfs_data_copy(src_path=existing_mounted_location, dst_path=new_mounted_location)
    src_external_table = make_table(schema_name=src_schema.name, external_csv=new_mounted_location)
    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)
    logger.info(f"dst_catalog={dst_catalog.name}, external_table={src_external_table.full_name}")
    table_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_external_table])
    udf_crawler = StaticUdfsCrawler(sql_backend, inventory_schema, [])
    grant_crawler = GrantsCrawler(table_crawler, udf_crawler)
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
    group_manager = GroupManager(sql_backend, ws, inventory_schema)
    migration_status_refresher = MigrationStatusRefresher(ws, sql_backend, inventory_schema, table_crawler)
    table_migrate = TablesMigrator(
        table_crawler,
        grant_crawler,
        ws,
        sql_backend,
        StaticTableMapping(ws, sql_backend, rules=rules),
        group_manager,
        migration_status_refresher,
    )

    table_migrate.migrate_tables()

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1
    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_external_table.name}").properties
    assert target_table_properties["upgraded_from"] == src_external_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())

    migration_status = MigrationStatusRefresher(ws, sql_backend, inventory_schema, table_crawler).snapshot()
    assert len(migration_status) == 1
    assert migration_status[0].src_schema == src_external_table.schema_name
    assert migration_status[0].src_table == src_external_table.name
    assert migration_status[0].dst_catalog == dst_catalog.name
    assert migration_status[0].dst_schema == dst_schema.name
    assert migration_status[0].dst_table == src_external_table.name


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_migrate_external_table_failed_sync(
    ws,
    caplog,
    sql_backend,
    inventory_schema,
    make_schema,
    make_table,
    env_or_skip,
):
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")

    src_schema = make_schema(catalog_name="hive_metastore")
    existing_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    src_external_table = make_table(schema_name=src_schema.name, external_csv=existing_mounted_location)
    table_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_external_table])
    grant_crawler = create_autospec(GrantsCrawler)
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
    group_manager = GroupManager(sql_backend, ws, inventory_schema)
    migration_status_refresher = MigrationStatusRefresher(ws, sql_backend, inventory_schema, table_crawler)
    table_migrate = TablesMigrator(
        table_crawler,
        grant_crawler,
        ws,
        sql_backend,
        StaticTableMapping(ws, sql_backend, rules=rules),
        group_manager,
        migration_status_refresher,
    )

    table_migrate.migrate_tables()
    assert "SYNC command failed to migrate" in caplog.text


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_revert_migrated_table(
    ws, sql_backend, inventory_schema, make_schema, make_table, make_catalog
):  # pylint: disable=too-many-locals
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
    udf_crawler = StaticUdfsCrawler(sql_backend, inventory_schema, [])
    grant_crawler = GrantsCrawler(table_crawler, udf_crawler)

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
    table_mapping = StaticTableMapping(ws, sql_backend, rules=rules)
    group_manager = GroupManager(sql_backend, ws, inventory_schema)
    migration_status_refresher = MigrationStatusRefresher(ws, sql_backend, inventory_schema, table_crawler)
    table_migrate = TablesMigrator(
        table_crawler, grant_crawler, ws, sql_backend, table_mapping, group_manager, migration_status_refresher
    )
    table_migrate.migrate_tables()

    table_migrate.revert_migrated_tables(src_schema1.name, delete_managed=True)

    # Checking that two of the tables were reverted and one was left intact.
    # The first two table belongs to schema 1 and should have not "upgraded_to" property
    assert not table_migrate.is_migrated(table_to_revert.schema_name, table_to_revert.name)
    # The second table didn't have the "upgraded_to" property set and should remain that way.
    assert not table_migrate.is_migrated(table_not_migrated.schema_name, table_not_migrated.name)
    # The third table belongs to schema2 and had the "upgraded_to" property set and should remain that way.
    assert table_migrate.is_migrated(table_to_not_revert.schema_name, table_to_not_revert.name)

    target_tables_schema1 = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema1.full_name}"))
    assert len(target_tables_schema1) == 0

    target_tables_schema2 = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema2.full_name}"))
    assert len(target_tables_schema2) == 1
    assert target_tables_schema2[0]["database"] == dst_schema2.name
    assert target_tables_schema2[0]["tableName"] == table_to_not_revert.name


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_mapping_skips_tables_databases(ws, sql_backend, inventory_schema, make_schema, make_table, make_catalog):
    src_schema1 = make_schema(catalog_name="hive_metastore")
    src_schema2 = make_schema(catalog_name="hive_metastore")
    table_to_migrate = make_table(schema_name=src_schema1.name)
    table_databricks_dataset = make_table(
        schema_name=src_schema1.name, external_csv="dbfs:/databricks-datasets/adult/adult.data"
    )
    table_to_skip = make_table(schema_name=src_schema1.name)
    table_in_skipped_database = make_table(schema_name=src_schema2.name)
    all_tables = [table_to_migrate, table_to_skip, table_in_skipped_database]

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
            table_to_migrate.name,
            table_to_migrate.name,
        ),
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema1.name,
            dst_schema1.name,
            table_to_skip.name,
            table_to_skip.name,
        ),
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema1.name,
            dst_schema1.name,
            table_databricks_dataset.name,
            table_databricks_dataset.name,
        ),
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema2.name,
            dst_schema2.name,
            table_in_skipped_database.name,
            table_in_skipped_database.name,
        ),
    ]
    table_mapping = StaticTableMapping(ws, sql_backend, rules=rules)
    table_mapping.skip_table(src_schema1.name, table_to_skip.name)
    table_mapping.skip_schema(src_schema2.name)
    assert len(table_mapping.get_tables_to_migrate(table_crawler)) == 1


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_mapping_reverts_table(
    ws, sql_backend, inventory_schema, make_schema, make_table, make_catalog
):  # pylint: disable=too-many-locals
    src_schema = make_schema(catalog_name="hive_metastore")
    table_to_revert = make_table(schema_name=src_schema.name)
    table_to_skip = make_table(schema_name=src_schema.name)
    all_tables = [
        table_to_revert,
        table_to_skip,
    ]

    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    table_crawler = StaticTablesCrawler(sql_backend, inventory_schema, all_tables)
    udf_crawler = StaticUdfsCrawler(sql_backend, inventory_schema, [])
    grant_crawler = GrantsCrawler(table_crawler, udf_crawler)
    rules = [
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema.name,
            dst_schema.name,
            table_to_skip.name,
            table_to_skip.name,
        ),
    ]
    table_mapping = StaticTableMapping(ws, sql_backend, rules=rules)
    migration_status_refresher = MigrationStatusRefresher(ws, sql_backend, inventory_schema, table_crawler)
    group_manager = GroupManager(sql_backend, ws, inventory_schema)
    table_migrate = TablesMigrator(
        table_crawler, grant_crawler, ws, sql_backend, table_mapping, group_manager, migration_status_refresher
    )
    table_migrate.migrate_tables()

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{table_to_skip.name}").properties
    assert target_table_properties["upgraded_from"] == table_to_skip.full_name

    sql_backend.execute(
        f"ALTER TABLE {table_to_revert.full_name} SET "
        f"TBLPROPERTIES('upgraded_to' = 'fake_catalog.fake_schema.fake_table');"
    )

    results = {_["key"]: _["value"] for _ in list(sql_backend.fetch(f"SHOW TBLPROPERTIES {table_to_revert.full_name}"))}
    assert "upgraded_to" in results
    assert results["upgraded_to"] == "fake_catalog.fake_schema.fake_table"

    rules2 = [
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema.name,
            dst_schema.name,
            table_to_skip.name,
            table_to_skip.name,
        ),
        Rule(
            "workspace",
            dst_catalog.name,
            src_schema.name,
            dst_schema.name,
            table_to_revert.name,
            table_to_revert.name,
        ),
    ]
    table_mapping2 = StaticTableMapping(ws, sql_backend, rules=rules2)
    mapping2 = table_mapping2.get_tables_to_migrate(table_crawler)

    # Checking to validate that table_to_skip was omitted from the list of rules
    assert len(mapping2) == 1
    assert mapping2[0].rule == Rule(
        "workspace",
        dst_catalog.name,
        src_schema.name,
        dst_schema.name,
        table_to_revert.name,
        table_to_revert.name,
    )
    results2 = {
        _["key"]: _["value"] for _ in list(sql_backend.fetch(f"SHOW TBLPROPERTIES {table_to_revert.full_name}"))
    }
    assert "upgraded_to" not in results2


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_migrate_managed_tables_with_acl(
    ws, sql_backend, inventory_schema, make_catalog, make_schema, make_table, make_user
):  # pylint: disable=too-many-locals
    if not ws.config.is_azure:
        pytest.skip("temporary: only works in azure test env")
    src_schema = make_schema(catalog_name="hive_metastore")
    src_managed_table = make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)
    user = make_user()
    src_grant = [
        Grant(principal=user.user_name, action_type="SELECT", table=src_managed_table.name, database=src_schema.name),
        Grant(principal=user.user_name, action_type="MODIFY", table=src_managed_table.name, database=src_schema.name),
    ]

    dst_catalog = make_catalog()
    dst_schema = make_schema(catalog_name=dst_catalog.name, name=src_schema.name)

    logger.info(f"dst_catalog={dst_catalog.name}, managed_table={src_managed_table.full_name}")

    table_crawler = StaticTablesCrawler(sql_backend, inventory_schema, [src_managed_table])
    udf_crawler = StaticUdfsCrawler(sql_backend, inventory_schema, [])
    grant_crawler = StaticGrantsCrawler(table_crawler, udf_crawler, src_grant)
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
    table_mapping = StaticTableMapping(ws, sql_backend, rules=rules)
    group_manager = GroupManager(sql_backend, ws, inventory_schema)
    migration_status_refresher = MigrationStatusRefresher(ws, sql_backend, inventory_schema, table_crawler)
    table_migrate = TablesMigrator(
        table_crawler, grant_crawler, ws, sql_backend, table_mapping, group_manager, migration_status_refresher
    )

    table_migrate.migrate_tables(acl_strategy=[AclMigrationWhat.LEGACY_TACL])

    target_tables = list(sql_backend.fetch(f"SHOW TABLES IN {dst_schema.full_name}"))
    assert len(target_tables) == 1

    target_table_properties = ws.tables.get(f"{dst_schema.full_name}.{src_managed_table.name}").properties
    target_table_grants = ws.grants.get(SecurableType.TABLE, f"{dst_schema.full_name}.{src_managed_table.name}")
    assert target_table_properties["upgraded_from"] == src_managed_table.full_name
    assert target_table_properties[Table.UPGRADED_FROM_WS_PARAM] == str(ws.get_workspace_id())
    assert target_table_grants.privilege_assignments[0].principal == user.user_name
    assert target_table_grants.privilege_assignments[0].privileges == [Privilege.MODIFY, Privilege.SELECT]
