import dataclasses
import json
import logging
import os
import sys
from collections.abc import Generator

import pytest
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk.errors.platform import InvalidParameterValue, PermissionDenied
from databricks.sdk.service.sql import SetWorkspaceWarehouseConfigRequestSecurityPolicy

from databricks.labs.ucx.azure.access import set_workspace_warehouse_config_wrapper
from databricks.labs.ucx.azure.resources import AzureAPIClient, AzureResource, StorageAccount
from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
)


logger = logging.getLogger(__name__)


@pytest.fixture
def skip_if_not_in_debug() -> None:
    if os.path.basename(sys.argv[0]) not in {"_jb_pytest_runner.py", "testlauncher.py"}:
        pytest.skip("This test can only be run in debug mode")


def test_azure_storage_accounts(skip_if_not_in_debug, env_or_skip, az_cli_ctx) -> None:
    mount_container = env_or_skip("TEST_MOUNT_CONTAINER")
    tables = [ExternalLocation(f"{mount_container}/folder1", 1)]
    az_cli_ctx.sql_backend.save_table(f"{az_cli_ctx.inventory_database}.external_locations", tables, ExternalLocation)

    az_cli_ctx.azure_resource_permissions.save_spn_permissions()

    mapping = az_cli_ctx.azure_resource_permissions.load()
    assert mapping[0].prefix == mount_container


def test_save_spn_permissions_local(skip_if_not_in_debug, env_or_skip, az_cli_ctx) -> None:
    tables = [ExternalLocation(f"{env_or_skip('TEST_MOUNT_CONTAINER')}/folder1", 1)]
    az_cli_ctx.sql_backend.save_table(f"{az_cli_ctx.inventory_database}.external_locations", tables, ExternalLocation)

    path = az_cli_ctx.azure_resource_permissions.save_spn_permissions()

    assert path is not None
    assert az_cli_ctx.workspace_client.workspace.get_status(path)


@pytest.fixture
def clean_up_spn(env_or_skip):
    # Making sure this test can only be launched from local
    env_or_skip("IDE_PROJECT_ROOTS")
    graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
    yield
    spns = graph_client.get("/v1.0/applications?$filter=startswith(displayName, 'UCXServicePrincipal')")['value']
    logging.debug("clearing ucx uber service principals")
    for spn in spns:
        try:
            graph_client.delete(f"/v1.0/applications(appId='{spn['appId']}')")
        except PermissionDenied:
            continue


def test_create_global_spn(skip_if_not_in_debug, env_or_skip, az_cli_ctx, make_cluster_policy, clean_up_spn) -> None:
    az_cli_ctx = az_cli_ctx.replace(azure_subscription_ids=[env_or_skip("TEST_AZURE_SUBSCRIPTION_ID")])
    policy = make_cluster_policy()
    az_cli_ctx.installation.save(dataclasses.replace(az_cli_ctx.config, policy_id=policy.policy_id))
    tables = [ExternalLocation(f"{env_or_skip('TEST_MOUNT_CONTAINER')}/folder1", 1)]
    az_cli_ctx.sql_backend.save_table(f"{az_cli_ctx.inventory_database}.external_locations", tables, ExternalLocation)
    prompts = MockPrompts({"Enter a name for the uber service principal to be created*": "UCXServicePrincipal"})

    az_cli_ctx.azure_resource_permissions.create_uber_principal(prompts)

    assert az_cli_ctx.config.uber_spn_id is not None
    policy_definition = json.loads(
        az_cli_ctx.workspace_client.cluster_policies.get(policy_id=policy.policy_id).definition
    )
    role_assignments = az_cli_ctx.azure_resource_permissions.role_assignments(env_or_skip("TEST_STORAGE_RESOURCE"))
    global_spn_assignment = None
    for assignment in role_assignments:
        if assignment.principal.client_id == az_cli_ctx.config.uber_spn_id:
            global_spn_assignment = assignment
            break
    assert global_spn_assignment
    assert global_spn_assignment.principal.client_id == az_cli_ctx.config.uber_spn_id
    assert global_spn_assignment.role_name == "Storage Blob Data Contributor"
    assert str(global_spn_assignment.scope) == env_or_skip("TEST_STORAGE_RESOURCE")
    assert (
        policy_definition["spark_conf.fs.azure.account.oauth2.client.id.labsazurethings.dfs.core.windows.net"]["value"]
        == az_cli_ctx.config.uber_spn_id
    )
    assert (
        policy_definition["spark_conf.fs.azure.account.oauth2.client.endpoint.labsazurethings.dfs.core.windows.net"][
            "value"
        ]
        == "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token"
    )


@pytest.fixture
def clean_warehouse_config(az_cli_ctx, is_in_debug) -> Generator[None, None, None]:
    """Clean workspace warehouse configuration."""
    warehouse_config = az_cli_ctx.workspace_client.warehouses.get_workspace_warehouse_config()
    yield
    security_policy = (
        SetWorkspaceWarehouseConfigRequestSecurityPolicy(warehouse_config.security_policy.value)
        if warehouse_config.security_policy
        else SetWorkspaceWarehouseConfigRequestSecurityPolicy.NONE
    )
    try:
        az_cli_ctx.workspace_client.warehouses.set_workspace_warehouse_config(
            data_access_config=warehouse_config.data_access_config,
            sql_configuration_parameters=warehouse_config.sql_configuration_parameters,
            security_policy=security_policy,
        )
    except InvalidParameterValue as e:
        if "enable_serverless_compute" not in str(e):
            raise
        set_workspace_warehouse_config_wrapper(
            az_cli_ctx.workspace_client.api_client,
            data_access_config=warehouse_config.data_access_config,
            sql_configuration_parameters=warehouse_config.sql_configuration_parameters,
            security_policy=security_policy,
            enable_serverless_compute=True,
        )


def test_add_service_principal_configuration_to_workspace_config(az_cli_ctx, clean_warehouse_config) -> None:
    # We mock Azure components to keep the testing scope to Databricks components
    az_cli_ctx = az_cli_ctx.replace(azure_subscription_ids=["test"])
    storage_account_id = AzureResource(
        "/subscriptions/test-subscription/resourceGroups/test-resource-group/providers/Microsoft.Storage/storageAccounts/teststorageaccount"
    )
    storage_account = StorageAccount(storage_account_id, "teststorageaccount", "west-europe", "Allow")

    # pylint: disable-next=protected-access
    az_cli_ctx.azure_resource_permissions._add_service_principal_configuration_to_workspace_warehouse_config(
        "test-principal-id",
        "secrets/ucx/test-principal-secret",
        [storage_account],
    )

    warehouse_config = az_cli_ctx.workspace_client.warehouses.get_workspace_warehouse_config()
    data_access_config = warehouse_config.data_access_config or []
    configuration_pairs = [(pair.key, pair.value) for pair in data_access_config]
    tenant_id = az_cli_ctx.azure_resources.tenant_id()
    endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    configuration_pairs_expected = [
        (
            "spark.hadoop.fs.azure.account.oauth2.client.id.teststorageaccount.dfs.core.windows.net",
            "test-principal-id",
        ),
        (
            "spark.hadoop.fs.azure.account.oauth.provider.type.teststorageaccount.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        (
            "spark.hadoop.fs.azure.account.oauth2.client.endpoint.teststorageaccount.dfs.core.windows.net",
            endpoint,
        ),
        (
            "spark.hadoop.fs.azure.account.auth.type.teststorageaccount.dfs.core.windows.net",
            "OAuth",
        ),
        (
            "spark.hadoop.fs.azure.account.oauth2.client.secret.teststorageaccount.dfs.core.windows.net",
            "{{secrets/ucx/test-principal-secret}}",
        ),
    ]
    missing_configuration_pairs = set(configuration_pairs_expected) - set(configuration_pairs)
    assert not missing_configuration_pairs, f"Missing configuration pairs: {missing_configuration_pairs}"


def test_create_global_service_principal_clean_up_after_failure(
    skip_if_not_in_debug,
    env_or_skip,
    az_cli_ctx,
    make_cluster_policy,
    clean_up_spn,
) -> None:
    storage_account_resource = AzureResource(env_or_skip("TEST_STORAGE_RESOURCE"))

    az_cli_ctx.workspace_client.api_client.do_original = az_cli_ctx.workspace_client.api_client.do

    def do_raise_permission_denied_on_put_warehouse_configuration(method: str, path: str, *args, **kwargs):
        if method == "PUT" and path == "/api/2.0/sql/config/warehouses":
            raise PermissionDenied("Cannot set warehouse configuration")
        return az_cli_ctx.workspace_client.api_client.do_original(method, path, *args, **kwargs)

    az_cli_ctx.workspace_client.api_client.do = do_raise_permission_denied_on_put_warehouse_configuration

    policy = make_cluster_policy()
    az_cli_ctx.installation.save(dataclasses.replace(az_cli_ctx.config, policy_id=policy.policy_id))
    tables = [ExternalLocation(f"{env_or_skip('TEST_MOUNT_CONTAINER')}/folder1", 1)]
    az_cli_ctx.sql_backend.save_table(f"{az_cli_ctx.inventory_database}.external_locations", tables, ExternalLocation)
    prompts = MockPrompts({"Enter a name for the uber service principal to be created*": "UCXServicePrincipal"})

    with pytest.raises(PermissionDenied):  # Raises the error again after cleaning up resources
        az_cli_ctx.azure_resource_permissions.create_uber_principal(prompts)

    assert az_cli_ctx.config.uber_spn_id is None

    ucx_scope = None
    for scope in az_cli_ctx.workspace_client.secrets.list_scopes():
        if scope.name == az_cli_ctx.config.inventory_database:
            ucx_scope = scope
            break
    assert ucx_scope is None

    policy_definition = json.loads(
        az_cli_ctx.workspace_client.cluster_policies.get(policy_id=policy.policy_id).definition
    )
    storage_account_name = storage_account_resource.storage_account
    missing_policy_keys = (
        f"spark_conf.fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net",
        f"spark_conf.fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",
        f"spark_conf.fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
        f"spark_conf.fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net",
        f"spark_conf.fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",
    )
    for key in missing_policy_keys:
        assert key not in policy_definition

    warehouse_config = az_cli_ctx.workspace_client.warehouses.get_workspace_warehouse_config()
    for config_pair in warehouse_config.data_access_config or []:
        for key in missing_policy_keys:
            assert key != config_pair.key, f"Warehouse config still contains policy key: {key}"

    # TODO: Test Azure resources, service principal and its role assignments
    # REASON: Missing permissions to test Azure resources
