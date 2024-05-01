import logging
from datetime import timedelta
import pytest

from databricks.labs.blueprint.tui import MockPrompts

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import DataSecurityMode, AwsAttributes
from databricks.sdk.service.catalog import Privilege, SecurableType, PrivilegeAssignment
from databricks.sdk.service.iam import PermissionLevel

from ..conftest import get_azure_spark_conf

logger = logging.getLogger(__name__)
_SPARK_CONF = get_azure_spark_conf()


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_create_catalog_schema_with_principal_acl_azure(
    ws, make_user, prepared_principal_acl, make_cluster_permissions, make_cluster
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
