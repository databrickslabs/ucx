import logging
from datetime import timedelta

import pytest
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import PermissionsList, SchemaInfo
from databricks.sdk.service.compute import DataSecurityMode, AwsAttributes
from databricks.sdk.service.catalog import Privilege, SecurableType, PrivilegeAssignment
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.mapping import Rule
from ..conftest import get_azure_spark_conf

logger = logging.getLogger(__name__)
_SPARK_CONF = get_azure_spark_conf()


@retried(on=[NotFound], timeout=timedelta(seconds=20))
def get_schema(ws: WorkspaceClient, full_name: str) -> SchemaInfo:
    return ws.schemas.get(full_name)


@retried(on=[NotFound], timeout=timedelta(seconds=20))
def get_schema_permissions_list(ws: WorkspaceClient, full_name: str) -> PermissionsList:
    return ws.grants.get(SecurableType.SCHEMA, full_name)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_create_ucx_catalog_creates_catalog(runtime_ctx, watchdog_remove_after) -> None:
    # Delete catalog created for testing to test the creation of a new catalog
    runtime_ctx.workspace_client.catalogs.delete(runtime_ctx.ucx_catalog, force=True)
    prompts = MockPrompts({f"Please provide storage location url for catalog: {runtime_ctx.ucx_catalog}": "metastore"})
    properties = {"RemoveAfter": watchdog_remove_after}

    runtime_ctx.catalog_schema.create_ucx_catalog(prompts, properties=properties)

    catalog_info = runtime_ctx.workspace_client.catalogs.get(runtime_ctx.ucx_catalog)
    assert catalog_info.name == runtime_ctx.ucx_catalog
    assert catalog_info.properties == properties


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_create_all_catalogs_schemas(ws: WorkspaceClient, runtime_ctx, make_random, watchdog_remove_after) -> None:
    """Create one catalog with two schemas mirroring the HIVE metastore schemas."""
    src_schema_1 = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_schema_2 = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_view = runtime_ctx.make_table(
        catalog_name=src_schema_1.catalog_name,
        schema_name=src_schema_1.name,
        ctas="SELECT 2+2 AS four",
        view=True,
    )
    src_table = runtime_ctx.make_table(catalog_name=src_schema_2.catalog_name, schema_name=src_schema_2.name)
    dst_catalog_name = f"ucx-{make_random()}"
    rules = [
        Rule("workspace", dst_catalog_name, src_schema_1.name, src_schema_1.name, src_view.name, src_view.name),
        Rule("workspace", dst_catalog_name, src_schema_2.name, src_schema_2.name, src_table.name, src_table.name),
    ]
    runtime_ctx.with_table_mapping_rules(rules)

    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})
    properties = {"RemoveAfter": watchdog_remove_after}
    runtime_ctx.catalog_schema.create_all_catalogs_schemas(mock_prompts, properties=properties)

    try:
        runtime_ctx.workspace_client.catalogs.get(dst_catalog_name)
    except NotFound:
        assert False, f"Catalog not created: {dst_catalog_name}"
    else:
        assert True, f"Catalog created: {dst_catalog_name}"
    for dst_schema_full_name in f"{dst_catalog_name}.{src_schema_1.name}", f"{dst_catalog_name}.{src_schema_2.name}":
        try:
            get_schema(ws, dst_schema_full_name)
        except RuntimeError:
            assert False, f"Schema not created: {dst_schema_full_name}"
        else:
            assert True, f"Schema created: {dst_schema_full_name}"


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
    ws: WorkspaceClient,
    runtime_ctx,
    make_random,
    make_user,
    watchdog_remove_after,
) -> None:
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    src_table = runtime_ctx.make_table(catalog_name=src_schema.catalog_name, schema_name=src_schema.name)
    dst_catalog_name = f"ucx-{make_random()}"
    dst_schema_name = "test"
    rules = [Rule("workspace", dst_catalog_name, src_schema.name, dst_schema_name, src_table.name, src_table.name)]
    runtime_ctx.with_table_mapping_rules(rules)

    schema_owner, table_owner = make_user(), make_user()
    grants = [
        Grant(schema_owner.user_name, "USAGE", src_schema.catalog_name, src_schema.name),
        Grant(table_owner.user_name, "USAGE", src_table.catalog_name, src_table.schema_name),
        Grant(schema_owner.user_name, "OWN", src_schema.catalog_name, src_schema.name),
        Grant(table_owner.user_name, "OWN", src_table.catalog_name, src_table.schema_name, src_table.name),
    ]
    for grant in grants:
        for sql in grant.hive_grant_sql():
            runtime_ctx.sql_backend.execute(sql)

    mock_prompts = MockPrompts({"Please provide storage location url for catalog: *": ""})
    properties = {"RemoveAfter": watchdog_remove_after}
    runtime_ctx.catalog_schema.create_all_catalogs_schemas(mock_prompts, properties=properties)

    schema_grants = get_schema_permissions_list(ws, f"{dst_catalog_name}.{dst_schema_name}")
    assert schema_grants.privilege_assignments is not None
    assert PrivilegeAssignment(table_owner.user_name, [Privilege.USE_SCHEMA]) in schema_grants.privilege_assignments
    assert get_schema(ws, f"{dst_catalog_name}.{dst_schema_name}").owner == schema_owner.user_name
