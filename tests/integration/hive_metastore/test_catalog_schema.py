import logging
from datetime import timedelta

import pytest
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo
from databricks.sdk.service.compute import DataSecurityMode, AwsAttributes
from databricks.sdk.service.catalog import Privilege, SecurableType, PrivilegeAssignment
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.hive_metastore.grants import Grant, GrantsCrawler
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
) -> None:
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
    ws,
    make_user,
    prepared_principal_acl,
    make_cluster_permissions,
    make_cluster,
    env_or_skip,
) -> None:
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
def test_create_catalog_schema_with_legacy_acls(
    ws,
    make_random,
    make_user,
    make_schema,
    make_mounted_location,
    runtime_ctx,
    sql_backend,
    watchdog_remove_after,
) -> None:
    src_schema = make_schema(catalog_name="hive_metastore")
    src_table = runtime_ctx.make_table(
        catalog_name=src_schema.catalog_name,
        schema_name=src_schema.name,
        external_csv=make_mounted_location,
    )
    dst_catalog_name = f"ucx-{make_random()}"
    dst_schema = SchemaInfo(catalog_name=dst_catalog_name, name="test", full_name=f"{dst_catalog_name}.test")
    rules = [Rule.from_src_dst(src_table, dst_schema)]
    runtime_ctx.with_table_mapping_rules(rules)
    runtime_ctx.with_dummy_resource_permission()

    user_a, user_b = make_user(), make_user()
    grants = [
        Grant(user_a.user_name, "USAGE", src_schema.catalog_name, src_schema.name),
        Grant(user_b.user_name, "SELECT", src_table.catalog_name, src_table.schema_name, src_table.name),
        Grant(user_a.user_name, "OWN", src_schema.catalog_name, src_schema.name),
        Grant(user_b.user_name, "OWN", src_table.catalog_name, src_table.schema_name, src_table.name),
    ]
    for grant in grants:
        for sql in grant.hive_grant_sql():
            sql_backend.execute(sql)

    # Ensure the view is populated (it's based on the crawled grants) and fetch the content.
    GrantsCrawler(runtime_ctx.tables_crawler, runtime_ctx.udfs_crawler).snapshot()

    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})
    properties = {"RemoveAfter": watchdog_remove_after}
    runtime_ctx.catalog_schema.create_all_catalogs_schemas(mock_prompts, properties=properties)

    schema_grants = ws.grants.get(SecurableType.SCHEMA, dst_schema.full_name)
    schema_grant = PrivilegeAssignment(user_a.user_name, [Privilege.USE_SCHEMA])
    assert schema_grant in schema_grants.privilege_assignments
    schema_info = ws.schemas.get(f"{dst_schema.full_name}")
    assert schema_info.owner == user_b.user_name
    schema_info = ws.schemas.get(dst_schema.full_name)
    assert schema_info.owner == user_a.user_name
