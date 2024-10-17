import logging
from datetime import timedelta

import pytest
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import CatalogInfo
from databricks.sdk.service.compute import DataSecurityMode, AwsAttributes
from databricks.sdk.service.catalog import Privilege, SecurableType, PrivilegeAssignment
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler
from databricks.labs.ucx.hive_metastore.mapping import Rule
from ..conftest import get_azure_spark_conf

logger = logging.getLogger(__name__)
_SPARK_CONF = get_azure_spark_conf()


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_create_ucx_catalog_creates_catalog(ws, runtime_ctx, watchdog_remove_after) -> None:
    # Delete catalog created for testing to test the creation of a new catalog
    runtime_ctx.workspace_client.catalogs.delete(runtime_ctx.ucx_catalog, force=True)
    prompts = MockPrompts({f"Please provide storage location url for catalog: {runtime_ctx.ucx_catalog}": "metastore"})

    runtime_ctx.catalog_schema.create_ucx_catalog(prompts, properties={"RemoveAfter": watchdog_remove_after})

    @retried(on=[NotFound], timeout=timedelta(seconds=20))
    def get_catalog(name: str) -> CatalogInfo:
        return ws.catalogs.get(name)

    assert get_catalog(runtime_ctx.ucx_catalog)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_create_catalog_schema_with_principal_acl_azure(
    ws,
    make_user,
    prepared_principal_acl,
    make_cluster_permissions,
    make_cluster,
):
    if not ws.config.is_azure:
        pytest.skip("only works in azure test env")
    ctx, _, schema_name, catalog_name = prepared_principal_acl

    cluster = make_cluster(single_node=True, spark_conf=_SPARK_CONF, data_security_mode=DataSecurityMode.NONE)
    user = make_user()
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=PermissionLevel.CAN_ATTACH_TO,
        user_name=user.user_name,
    )
    catalog_schema = ctx.catalog_schema
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    schema_grants = ws.grants.get(SecurableType.SCHEMA, schema_name)
    catalog_grants = ws.grants.get(SecurableType.CATALOG, catalog_name)
    schema_grant = PrivilegeAssignment(user.user_name, [Privilege.USE_SCHEMA])
    catalog_grant = PrivilegeAssignment(user.user_name, [Privilege.USE_CATALOG])
    assert schema_grant in schema_grants.privilege_assignments
    assert catalog_grant in catalog_grants.privilege_assignments


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_create_catalog_schema_with_principal_acl_aws(
    ws, make_user, prepared_principal_acl, make_cluster_permissions, make_cluster, env_or_skip
):
    ctx, _, schema_name, catalog_name = prepared_principal_acl

    cluster = make_cluster(
        single_node=True,
        data_security_mode=DataSecurityMode.NONE,
        aws_attributes=AwsAttributes(instance_profile_arn=env_or_skip("TEST_WILDCARD_INSTANCE_PROFILE")),
    )
    user = make_user()
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=PermissionLevel.CAN_ATTACH_TO,
        user_name=user.user_name,
    )
    catalog_schema = ctx.catalog_schema
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    schema_grants = ws.grants.get(SecurableType.SCHEMA, schema_name)
    catalog_grants = ws.grants.get(SecurableType.CATALOG, catalog_name)
    schema_grant = PrivilegeAssignment(user.user_name, [Privilege.USE_SCHEMA])
    catalog_grant = PrivilegeAssignment(user.user_name, [Privilege.USE_CATALOG])
    assert schema_grant in schema_grants.privilege_assignments
    assert catalog_grant in catalog_grants.privilege_assignments


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_create_catalog_schema_with_legacy_hive_metastore_privileges(
    ws: WorkspaceClient,
    runtime_ctx,
    make_random,
    make_user,
    watchdog_remove_after,
) -> None:
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_table = runtime_ctx.make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)
    dst_catalog_name = f"ucx_{make_random()}"
    dst_schema_name = "test"
    rules = [Rule("workspace", dst_catalog_name, src_schema.name, dst_schema_name, src_table.name, src_table.name)]
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()

    user_a = make_user()
    user_b = make_user()

    sql_backend.execute(f"GRANT USAGE ON DATABASE {src_schema.name} TO `{user_a.user_name}`;")
    sql_backend.execute(f"GRANT SELECT ON {src_external_table.full_name} TO `{user_b.user_name}`;")
    sql_backend.execute(f"ALTER DATABASE {src_schema.name} OWNER TO `{user_b.user_name}`;")
    sql_backend.execute(f"ALTER TABLE {src_external_table.full_name} OWNER TO `{user_a.user_name}`;")

    # Ensure the view is populated (it's based on the crawled grants) and fetch the content.
    GrantsCrawler(runtime_ctx.tables_crawler, runtime_ctx.udfs_crawler).snapshot()

    catalog_schema = runtime_ctx.catalog_schema
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})
    catalog_schema.create_all_catalogs_schemas(mock_prompts)

    schema_grants = ws.grants.get(SecurableType.SCHEMA, f"{dst_catalog.name}.{dst_schema.name}")
    schema_grant = PrivilegeAssignment(user_a.user_name, [Privilege.USE_SCHEMA])
    assert schema_grant in schema_grants.privilege_assignments
    schema_info = ws.schemas.get(f"{dst_schema.full_name}")
    assert schema_info.owner == user_b.user_name


def test_create_catalog_schema_when_users_group_in_warehouse_acl(
    caplog,
    runtime_ctx,
    make_random,
    make_warehouse,
    make_warehouse_permissions,
) -> None:
    """Privileges inferred from being a member of the 'users' group are ignored."""
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_table = runtime_ctx.make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)
    dst_catalog_name = f"ucx_{make_random()}"
    dst_schema_name = "test"
    rule = Rule("workspace", dst_catalog_name, src_schema.name, dst_schema_name, src_table.name, src_table.name)
    runtime_ctx.with_table_mapping_rules([rule])
    runtime_ctx.with_dummy_resource_permission()
    runtime_ctx.make_group()
    warehouse = make_warehouse()
    make_warehouse_permissions(object_id=warehouse.id, permission_level=PermissionLevel.CAN_USE, group_name="users")
    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.aggregate"):
        runtime_ctx.catalog_schema.create_all_catalogs_schemas(mock_prompts)

    failed_to_migrate_message = (
        f"failed-to-migrate: Failed to migrate ACL for {src_schema.full_name} to {dst_catalog_name}"
    )
    assert failed_to_migrate_message not in caplog.messages
