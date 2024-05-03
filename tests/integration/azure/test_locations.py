import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk.errors.platform import NotFound
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.locations import ExternalLocationsMigration
from databricks.sdk.service.compute import DataSecurityMode
from databricks.labs.ucx.azure.resources import AzureAPIClient, AzureResources
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.sdk.service.catalog import SecurableType, PermissionsChange, Privilege, PrivilegeAssignment

from databricks.labs.ucx.hive_metastore.locations import ExternalLocation
from ..conftest import get_azure_spark_conf

_SPARK_CONF = get_azure_spark_conf()


def save_delete_location(ws, name):
    try:
        ws.external_locations.delete(name, force=True)
    except NotFound:
        # If test failed with exception threw before external location is created,
        # don't fail the test with external location cannot be deleted error,
        # instead let the original exception be reported.
        pass


@pytest.mark.skip
def test_run(caplog, ws, sql_backend, inventory_schema, az_cli_ctx):
    locations = [
        ExternalLocation("abfss://uctest@ziyuanqintest.dfs.core.windows.net/one", 1),
        ExternalLocation("abfss://uctest@ziyuanqintest.dfs.core.windows.net/two", 2),
        ExternalLocation("abfss://ucx2@ziyuanqintest.dfs.core.windows.net/", 2),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", locations, ExternalLocation)
    location_crawler = ExternalLocations(ws, sql_backend, inventory_schema)

    installation = MockInstallation(
        {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://uctest@ziyuanqintest.dfs.core.windows.net/',
                    'client_id': "redacted-for-github-929e765443eb",
                    'principal': "oneenv-adls",
                    'privilege': "WRITE_FILES",
                    'type': "Application",
                },
                {
                    'prefix': 'abfss://ucx2@ziyuanqintest.dfs.core.windows.net/',
                    'client_id': "redacted-for-github-ebcef6708997",
                    'principal': "ziyuan-user-assigned-mi",
                    'privilege': "WRITE_FILES",
                    'type': "ManagedIdentity",
                },
            ]
        }
    )

    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azurerm = AzureResources(azure_mgmt_client, graph_client)
    azure_resource_permissions = AzureResourcePermissions(installation, ws, azurerm, location_crawler)
    location_migration = ExternalLocationsMigration(
        ws,
        location_crawler,
        azure_resource_permissions,
        azurerm,
        az_cli_ctx.principal_acl,
    )
    try:
        location_migration.run()
        assert "All UC external location are created." in caplog.text
        assert ws.external_locations.get("uctest_ziyuanqintest_one").credential_name == "oneenv-adls"
        assert ws.external_locations.get("uctest_ziyuanqintest_two").credential_name == "oneenv-adls"
        assert ws.external_locations.get("ucx2_ziyuanqintest").credential_name == "ziyuan-user-assigned-mi"
    finally:
        save_delete_location(ws, "uctest_ziyuanqintest_one")
        save_delete_location(ws, "uctest_ziyuanqintest_two")
        save_delete_location(ws, "ucx2_ziyuanqintest")


@pytest.mark.skip
def test_read_only_location(caplog, ws, sql_backend, inventory_schema, az_cli_ctx):
    locations = [ExternalLocation("abfss://ucx1@ziyuanqintest.dfs.core.windows.net/", 1)]
    sql_backend.save_table(f"{inventory_schema}.external_locations", locations, ExternalLocation)
    location_crawler = ExternalLocations(ws, sql_backend, inventory_schema)

    installation = MockInstallation(
        {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://ucx1@ziyuanqintest.dfs.core.windows.net/',
                    'client_id': "redacted-for-github-ff66ffe1d728",
                    'principal': "ziyuanqin-uc-test-ac",
                    'privilege': "READ_FILES",
                    'type': "ManagedIdentity",
                }
            ]
        }
    )

    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azurerm = AzureResources(azure_mgmt_client, graph_client)
    azure_resource_permissions = AzureResourcePermissions(installation, ws, azurerm, location_crawler)

    location_migration = ExternalLocationsMigration(
        ws,
        location_crawler,
        azure_resource_permissions,
        azurerm,
        az_cli_ctx.principal_acl,
    )
    try:
        location_migration.run()
        assert ws.external_locations.get("ucx1_ziyuanqintest").credential_name == "ziyuanqin-uc-test-ac"
        assert ws.external_locations.get("ucx1_ziyuanqintest").read_only
    finally:
        save_delete_location(ws, "ucx1_ziyuanqintest")


@pytest.mark.skip
def test_missing_credential(caplog, ws, sql_backend, inventory_schema, az_cli_ctx):
    locations = [
        ExternalLocation("abfss://ucx3@ziyuanqintest.dfs.core.windows.net/one", 1),
        ExternalLocation("abfss://ucx3@ziyuanqintest.dfs.core.windows.net/two", 2),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", locations, ExternalLocation)
    location_crawler = ExternalLocations(ws, sql_backend, inventory_schema)

    installation = MockInstallation(
        {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://ucx3@ziyuanqintest.dfs.core.windows.net/',
                    'client_id': "no-such-id",
                    'principal': "dummy_principal",
                    'privilege': "WRITE_FILES",
                    'type': "Application",
                },
            ]
        }
    )

    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azurerm = AzureResources(azure_mgmt_client, graph_client)
    azure_resource_permissions = AzureResourcePermissions(installation, ws, azurerm, location_crawler)
    location_migration = ExternalLocationsMigration(
        ws,
        location_crawler,
        azure_resource_permissions,
        azurerm,
        az_cli_ctx.principal_acl,
    )
    leftover_loc = location_migration.run()

    assert "External locations below are not created in UC" in caplog.text
    assert len(leftover_loc) == 2


@pytest.mark.skip
def test_overlapping_location(caplog, ws, sql_backend, inventory_schema, az_cli_ctx):
    """Customer may already create external location with url that is a sub path of the table prefix hive_metastore/locations.py extracted.
    This test case is to verify the overlapping location will be detected and reported.
    """
    # create an external location first so the overlapping conflict will be triggered latter
    ws.external_locations.create(
        "uctest_ziyuanqintest_overlap", "abfss://uctest@ziyuanqintest.dfs.core.windows.net/a", "oneenv-adls"
    )

    locations = [ExternalLocation("abfss://uctest@ziyuanqintest.dfs.core.windows.net/", 1)]
    sql_backend.save_table(f"{inventory_schema}.external_locations", locations, ExternalLocation)
    location_crawler = ExternalLocations(ws, sql_backend, inventory_schema)

    installation = MockInstallation(
        {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'abfss://uctest@ziyuanqintest.dfs.core.windows.net/',
                    'client_id': "redacted-for-github-929e765443eb",
                    'principal': "oneenv-adls",
                    'privilege': "WRITE_FILES",
                    'type': "Application",
                }
            ]
        }
    )

    azure_mgmt_client = AzureAPIClient(
        ws.config.arm_environment.resource_manager_endpoint,
        ws.config.arm_environment.service_management_endpoint,
    )
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    azurerm = AzureResources(azure_mgmt_client, graph_client)
    azure_resource_permissions = AzureResourcePermissions(installation, ws, azurerm, location_crawler)
    location_migration = ExternalLocationsMigration(
        ws,
        location_crawler,
        azure_resource_permissions,
        azurerm,
        az_cli_ctx.principal_acl,
    )
    try:
        leftover_loc_urls = location_migration.run()
        assert "abfss://uctest@ziyuanqintest.dfs.core.windows.net/" in leftover_loc_urls
        assert "overlaps with an existing external location" in caplog.text
    finally:
        save_delete_location(ws, "uctest_ziyuanqintest_overlap")


def test_run_validate_acl(make_cluster_permissions, ws, make_user, make_cluster, az_cli_ctx, env_or_skip):
    az_cli_ctx.with_dummy_resource_permission()
    az_cli_ctx.save_locations()
    cluster = make_cluster(single_node=True, spark_conf=_SPARK_CONF, data_security_mode=DataSecurityMode.NONE)
    user = make_user()
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=PermissionLevel.CAN_ATTACH_TO,
        user_name=user.user_name,
    )

    location_migration = az_cli_ctx.external_locations_migration
    try:
        location_migration.run()
        permissions = ws.grants.get(
            SecurableType.EXTERNAL_LOCATION, env_or_skip("TEST_A_LOCATION"), principal=user.user_name
        )
        expected_azure_permission = PrivilegeAssignment(
            principal=user.user_name,
            privileges=[Privilege.CREATE_EXTERNAL_TABLE, Privilege.CREATE_EXTERNAL_VOLUME, Privilege.READ_FILES],
        )
        assert expected_azure_permission in permissions.privilege_assignments
    finally:
        remove_azure_permissions = [
            Privilege.CREATE_EXTERNAL_TABLE,
            Privilege.CREATE_EXTERNAL_VOLUME,
            Privilege.READ_FILES,
        ]
        ws.grants.update(
            SecurableType.EXTERNAL_LOCATION,
            env_or_skip("TEST_A_LOCATION"),
            changes=[PermissionsChange(remove=remove_azure_permissions, principal=user.user_name)],
        )
