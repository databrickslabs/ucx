import io
import logging
import re
from unittest.mock import MagicMock, create_autospec

import pytest
import yaml
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError, NotFound, ResourceDoesNotExist
from databricks.sdk.errors.platform import InvalidParameterValue
from databricks.sdk.service.catalog import (
    AwsIamRole,
    AzureManagedIdentity,
    AzureServicePrincipal,
    StorageCredentialInfo,
    ValidateStorageCredentialResponse,
)
from databricks.sdk.service.workspace import GetSecretResponse

from databricks.labs.ucx.assessment.azure import (
    AzureServicePrincipalCrawler,
    AzureServicePrincipalInfo,
)
from databricks.labs.ucx.azure.access import (
    AzureResourcePermissions,
    StoragePermissionMapping,
)
from databricks.labs.ucx.azure.credentials import (
    ServicePrincipalMigration,
    ServicePrincipalMigrationInfo,
    StorageCredentialManager,
)


@pytest.fixture
def ws():
    state = {
        "/Users/foo/.ucx/config.yml": yaml.dump(
            {
                'version': 2,
                'inventory_database': 'ucx',
                'warehouse_id': 'test',
                'connect': {
                    'host': 'foo',
                    'token': 'bar',
                },
            }
        ),
    }

    def download(path: str) -> io.StringIO:
        if path not in state:
            raise NotFound(path)
        return io.StringIO(state[path])

    ws_mock = create_autospec(WorkspaceClient)
    ws_mock.config.host = 'https://localhost'
    ws_mock.current_user.me().user_name = "foo"
    ws_mock.workspace.download = download
    return ws_mock


def side_effect_create_storage_credential(name, azure_service_principal, comment, read_only):
    return StorageCredentialInfo(
        name=name, azure_service_principal=azure_service_principal, comment=comment, read_only=read_only
    )


def side_effect_validate_storage_credential(storage_credential_name, url, read_only):  # pylint: disable=unused-argument
    if "overlap" in storage_credential_name:
        raise InvalidParameterValue
    if read_only:
        response = {"isDir": True, "results": [{"message": "", "operation": "READ", "result": "PASS"}]}
        return ValidateStorageCredentialResponse.from_dict(response)
    response = {"isDir": True, "results": [{"message": "", "operation": "WRITE", "result": "PASS"}]}
    return ValidateStorageCredentialResponse.from_dict(response)


@pytest.fixture
def credential_manager(ws):
    ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(aws_iam_role=AwsIamRole(role_arn="arn:aws:iam::123456789012:role/example-role-name")),
        StorageCredentialInfo(
            azure_managed_identity=AzureManagedIdentity("/subscriptions/.../providers/Microsoft.Databricks/...")
        ),
        StorageCredentialInfo(
            azure_service_principal=AzureServicePrincipal(
                application_id="b6420590-5e1c-4426-8950-a94cbe9b6115",
                directory_id="62e43d7d-df53-4c64-86ed-c2c1a3ac60c3",
                client_secret="secret",
            )
        ),
        StorageCredentialInfo(
            azure_service_principal=AzureServicePrincipal(
                application_id="app_secret2",
                directory_id="directory_id_1",
                client_secret="secret",
            )
        ),
    ]

    ws.storage_credentials.create.side_effect = side_effect_create_storage_credential
    ws.storage_credentials.validate.side_effect = side_effect_validate_storage_credential

    return StorageCredentialManager(ws)


def test_list_storage_credentials(credential_manager):
    assert {"b6420590-5e1c-4426-8950-a94cbe9b6115", "app_secret2"} == credential_manager.list_storage_credentials()


def test_create_storage_credentials(credential_manager):
    sp_1 = ServicePrincipalMigrationInfo(
        StoragePermissionMapping(
            prefix="prefix1",
            client_id="app_secret1",
            principal="principal_write",
            privilege="WRITE_FILES",
            directory_id="directory_id_1",
        ),
        "test",
    )
    sp_2 = ServicePrincipalMigrationInfo(
        StoragePermissionMapping(
            prefix="prefix2",
            client_id="app_secret2",
            principal="principal_read",
            privilege="READ_FILES",
            directory_id="directory_id_1",
        ),
        "test",
    )

    storage_credential = credential_manager.create_storage_credential(sp_1)
    assert sp_1.permission_mapping.principal == storage_credential.name
    assert storage_credential.read_only is False

    storage_credential = credential_manager.create_storage_credential(sp_2)
    assert sp_2.permission_mapping.principal == storage_credential.name
    assert storage_credential.read_only is True


def test_validate_storage_credentials(credential_manager):
    sp_1 = ServicePrincipalMigrationInfo(
        StoragePermissionMapping(
            prefix="prefix1",
            client_id="app_secret1",
            principal="principal_1",
            privilege="WRITE_FILES",
            directory_id="directory_id_1",
        ),
        "test",
    )
    sc_1 = StorageCredentialInfo(
        name=sp_1.permission_mapping.principal,
        azure_service_principal=AzureServicePrincipal(
            sp_1.permission_mapping.directory_id, sp_1.permission_mapping.client_id, sp_1.client_secret
        ),
        read_only=False,
    )

    sp_2 = ServicePrincipalMigrationInfo(
        StoragePermissionMapping(
            prefix="prefix2",
            client_id="app_secret2",
            principal="principal_read",
            privilege="READ_FILES",
            directory_id="directory_id_1",
        ),
        "test",
    )
    sc_2 = StorageCredentialInfo(
        name=sp_2.permission_mapping.principal,
        azure_service_principal=AzureServicePrincipal(
            sp_2.permission_mapping.directory_id, sp_2.permission_mapping.client_id, sp_2.client_secret
        ),
        read_only=True,
    )

    sp_3 = ServicePrincipalMigrationInfo(
        StoragePermissionMapping(
            prefix="overlap_with_external_location",
            client_id="app_secret4",
            principal="principal_overlap",
            privilege="WRITE_FILES",
            directory_id="directory_id_2",
        ),
        "test",
    )
    sc_3 = StorageCredentialInfo(
        name=sp_3.permission_mapping.principal,
        azure_service_principal=AzureServicePrincipal(
            sp_3.permission_mapping.directory_id, sp_3.permission_mapping.client_id, sp_3.client_secret
        ),
    )

    # validate normal storage credential
    validation = credential_manager.validate_storage_credential(sc_1, sp_1)
    assert validation.read_only is False
    assert validation.name == sp_1.permission_mapping.principal
    for result in validation.results:
        if result.operation.value == "WRITE":
            assert result.result.value == "PASS"

    # validate read-only storage credential
    validation = credential_manager.validate_storage_credential(sc_2, sp_2)
    assert validation.read_only is True
    assert validation.name == sp_2.permission_mapping.principal
    for result in validation.results:
        if result.operation.value == "READ":
            assert result.result.value == "PASS"

    # prefix used for validation overlaps with existing external location
    validation = credential_manager.validate_storage_credential(sc_3, sp_3)
    assert (
        validation.results[0].message
        == "The validation is skipped because an existing external location overlaps with the location used for validation."
    )


@pytest.fixture
def sp_migration(ws, credential_manager):
    ws.secrets.get_secret.return_value = GetSecretResponse(value="aGVsbG8gd29ybGQ=")

    arp = create_autospec(AzureResourcePermissions)
    arp.load.return_value = [
        StoragePermissionMapping(
            prefix="prefix1",
            client_id="app_secret1",
            principal="principal_1",
            privilege="WRITE_FILES",
            directory_id="directory_id_1",
        ),
        StoragePermissionMapping(
            prefix="prefix2",
            client_id="app_secret2",
            principal="principal_read",
            privilege="READ_FILES",
            directory_id="directory_id_1",
        ),
        StoragePermissionMapping(
            prefix="prefix3",
            client_id="app_secret3",
            principal="principal_write",
            privilege="WRITE_FILES",
            directory_id="directory_id_2",
        ),
        StoragePermissionMapping(
            prefix="overlap_with_external_location",
            client_id="app_secret4",
            principal="principal_overlap",
            privilege="WRITE_FILES",
            directory_id="directory_id_2",
        ),
    ]

    sp_crawler = create_autospec(AzureServicePrincipalCrawler)
    sp_crawler.snapshot.return_value = [
        AzureServicePrincipalInfo("app_secret1", "test_scope", "test_key", "tenant_id_1", "storage1"),
        AzureServicePrincipalInfo("app_secret2", "test_scope", "test_key", "tenant_id_1", "storage1"),
        AzureServicePrincipalInfo("app_secret3", "test_scope", "", "tenant_id_2", "storage1"),
        AzureServicePrincipalInfo("app_secret4", "", "", "tenant_id_2", "storage1"),
    ]

    return ServicePrincipalMigration(MockInstallation(), ws, arp, sp_crawler, credential_manager)


def test_for_cli_not_azure(caplog, ws):
    ws.config.is_azure = False
    with pytest.raises(SystemExit):
        ServicePrincipalMigration.for_cli(ws, MagicMock())
    assert "Workspace is not on azure, please run this command on azure databricks workspaces." in caplog.text


def test_for_cli_not_prompts(ws):
    ws.config.is_azure = True
    prompts = MockPrompts({"Have you reviewed the azure_storage_account_info.csv *": "No"})
    with pytest.raises(SystemExit):
        ServicePrincipalMigration.for_cli(ws, prompts)


def test_for_cli(ws):
    ws.config.is_azure = True
    prompts = MockPrompts({"Have you reviewed the azure_storage_account_info.csv *": "Yes"})

    assert isinstance(ServicePrincipalMigration.for_cli(ws, prompts), ServicePrincipalMigration)


@pytest.mark.parametrize(
    "secret_bytes_value, num_migrated",
    [
        (GetSecretResponse(value="aGVsbG8gd29ybGQ="), 1),
        (GetSecretResponse(value="T2zhLCBNdW5kbyE="), 0),
        (GetSecretResponse(value=None), 0),
    ],
)
def test_read_secret_value_decode(ws, sp_migration, secret_bytes_value, num_migrated):
    ws.secrets.get_secret.return_value = secret_bytes_value

    prompts = MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials*": "Yes"})
    assert len(sp_migration.run(prompts)) == num_migrated


@pytest.mark.parametrize(
    "exception, log_pattern, num_migrated",
    [
        (ResourceDoesNotExist(), r"Secret.* does not exists", 0),
        (InternalError(), r"InternalError while reading secret .*", 0),
    ],
)
def test_read_secret_read_exception(caplog, ws, sp_migration, exception, log_pattern, num_migrated):
    caplog.set_level(logging.INFO)
    ws.secrets.get_secret.side_effect = exception

    prompts = MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials*": "Yes"})

    assert len(sp_migration.run(prompts)) == num_migrated
    assert re.search(log_pattern, caplog.text)


def test_print_action_plan(caplog, ws, sp_migration):
    caplog.set_level(logging.INFO)
    ws.secrets.get_secret.return_value = GetSecretResponse(value="aGVsbG8gd29ybGQ=")

    prompts = MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials*": "Yes"})

    sp_migration.run(prompts)

    log_pattern = r"Service Principal name: .* application_id: .* privilege .* on location .*"
    for msg in caplog.messages:
        if re.search(log_pattern, msg):
            assert True
            return
    assert False, "Action plan is not logged"


def test_run_without_confirmation(ws, sp_migration):
    ws.secrets.get_secret.return_value = GetSecretResponse(value="aGVsbG8gd29ybGQ=")
    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "No",
        }
    )

    assert sp_migration.run(prompts) == []


def test_run(ws, sp_migration):
    prompts = MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials*": "Yes"})

    results = sp_migration.run(prompts)
    for result in results:
        if result.name != "principal_1":
            assert (
                False
            ), "Service principal with no client_secret in databricks secret or already be used in storage credential should not be migrated"
