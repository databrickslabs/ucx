import base64
import logging
import re
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.errors.platform import InvalidParameterValue
from databricks.sdk.service.catalog import (
    AwsIamRole,
    AzureManagedIdentity,
    AzureServicePrincipal,
    StorageCredentialInfo,
    ValidateStorageCredentialResponse,
    ValidationResult,
    ValidationResultOperation,
    ValidationResultResult,
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
from databricks.labs.ucx.azure.resources import AzureResources
from databricks.labs.ucx.hive_metastore import ExternalLocations
from tests.unit import DEFAULT_CONFIG


@pytest.fixture
def ws():
    return create_autospec(WorkspaceClient)


@pytest.fixture
def installation():
    return MockInstallation(
        DEFAULT_CONFIG
        | {
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'prefix1',
                    'client_id': 'app_secret1',
                    'principal': 'principal_1',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_1',
                },
                {
                    'prefix': 'prefix2',
                    'client_id': 'app_secret2',
                    'principal': 'principal_read',
                    'privilege': 'READ_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_1',
                },
                {
                    'prefix': 'prefix3',
                    'client_id': 'app_secret3',
                    'principal': 'principal_write',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_2',
                },
                {
                    'prefix': 'overlap_with_external_location',
                    'client_id': 'app_secret4',
                    'principal': 'principal_overlap',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_2',
                },
                {
                    'prefix': 'prefix5',
                    'client_id': 'app_secret4',
                    'principal': 'managed_identity',
                    'privilege': 'WRITE_FILES',
                    'type': 'ManagedIdentity',
                },
            ],
        }
    )


def side_effect_create_storage_credential(name, azure_service_principal, comment, read_only):
    return StorageCredentialInfo(
        name=name, azure_service_principal=azure_service_principal, comment=comment, read_only=read_only
    )


def side_effect_validate_storage_credential(storage_credential_name, url, read_only):  # pylint: disable=unused-argument
    if "overlap" in storage_credential_name:
        raise InvalidParameterValue
    if "none" in storage_credential_name:
        return ValidateStorageCredentialResponse()
    if "fail" in storage_credential_name:
        return ValidateStorageCredentialResponse(
            is_dir=True,
            results=[
                ValidationResult(
                    operation=ValidationResultOperation.LIST, result=ValidationResultResult.FAIL, message="fail"
                ),
                ValidationResult(operation=None, result=ValidationResultResult.FAIL, message="fail"),
            ],
        )
    if read_only:
        return ValidateStorageCredentialResponse(
            is_dir=True,
            results=[ValidationResult(operation=ValidationResultOperation.READ, result=ValidationResultResult.PASS)],
        )
    return ValidateStorageCredentialResponse(
        is_dir=True,
        results=[ValidationResult(operation=ValidationResultOperation.WRITE, result=ValidationResultResult.PASS)],
    )


@pytest.fixture
def credential_manager(ws):
    ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(aws_iam_role=AwsIamRole("arn:aws:iam::123456789012:role/example-role-name")),
        StorageCredentialInfo(
            azure_managed_identity=AzureManagedIdentity("/subscriptions/.../providers/Microsoft.Databricks/...")
        ),
        StorageCredentialInfo(
            name="included_test",
            azure_service_principal=AzureServicePrincipal(
                "62e43d7d-df53-4c64-86ed-c2c1a3ac60c3",
                "b6420590-5e1c-4426-8950-a94cbe9b6115",
                "secret",
            ),
        ),
        StorageCredentialInfo(azure_service_principal=AzureServicePrincipal("directory_id_1", "app_secret2", "secret")),
    ]

    ws.storage_credentials.create.side_effect = side_effect_create_storage_credential
    ws.storage_credentials.validate.side_effect = side_effect_validate_storage_credential

    return StorageCredentialManager(ws)


def test_list_storage_credentials(credential_manager):
    assert credential_manager.list() == {"b6420590-5e1c-4426-8950-a94cbe9b6115", "app_secret2"}


def test_list_included_storage_credentials(credential_manager):
    include_names = {"included_test"}
    assert credential_manager.list(include_names) == {"b6420590-5e1c-4426-8950-a94cbe9b6115"}


def test_create_storage_credentials(credential_manager):
    sp_1 = ServicePrincipalMigrationInfo(
        StoragePermissionMapping(
            "prefix1",
            "app_secret1",
            "principal_write",
            "WRITE_FILES",
            "Application",
            "directory_id_1",
        ),
        "test",
    )
    sp_2 = ServicePrincipalMigrationInfo(
        StoragePermissionMapping(
            "prefix2",
            "app_secret2",
            "principal_read",
            "READ_FILES",
            "Application",
            "directory_id_1",
        ),
        "test",
    )

    storage_credential = credential_manager.create_with_client_secret(sp_1)
    assert sp_1.permission_mapping.principal == storage_credential.name
    assert storage_credential.read_only is False

    storage_credential = credential_manager.create_with_client_secret(sp_2)
    assert sp_2.permission_mapping.principal == storage_credential.name
    assert storage_credential.read_only is True


def test_validate_storage_credentials(credential_manager):
    permission_mapping = StoragePermissionMapping(
        "prefix", "client_id", "principal_1", "WRITE_FILES", "Application", "directory_id"
    )

    # validate normal storage credential
    validation = credential_manager.validate(permission_mapping)
    assert validation.read_only is False
    assert validation.name == permission_mapping.principal
    assert not validation.failures


def test_validate_read_only_storage_credentials(credential_manager):
    permission_mapping = StoragePermissionMapping(
        "prefix", "client_id", "principal_read", "READ_FILES", "Application", "directory_id_1"
    )

    # validate read-only storage credential
    validation = credential_manager.validate(permission_mapping)
    assert validation.read_only is True
    assert validation.name == permission_mapping.principal
    assert not validation.failures


def test_validate_storage_credentials_overlap_location(credential_manager):
    permission_mapping = StoragePermissionMapping(
        "prefix", "client_id", "overlap", "WRITE_FILES", "Application", "directory_id_2"
    )

    # prefix used for validation overlaps with existing external location will raise InvalidParameterValue
    # assert InvalidParameterValue is handled
    validation = credential_manager.validate(permission_mapping)
    assert validation.failures == [
        "The validation is skipped because an existing external location overlaps with the location used for validation."
    ]


def test_validate_storage_credentials_non_response(credential_manager):
    permission_mapping = StoragePermissionMapping(
        "prefix", "client_id", "none", "WRITE_FILES", "Application", "directory_id"
    )

    validation = credential_manager.validate(permission_mapping)
    assert validation.failures == ["Validation returned no results."]


def test_validate_storage_credentials_failed_operation(credential_manager):
    permission_mapping = StoragePermissionMapping(
        "prefix", "client_id", "fail", "WRITE_FILES", "Application", "directory_id_2"
    )

    validation = credential_manager.validate(permission_mapping)
    assert validation.failures == ["LIST validation failed with message: fail"]


@pytest.fixture
def sp_migration(ws, installation, credential_manager):
    ws.secrets.get_secret.return_value = GetSecretResponse(
        value=base64.b64encode("hello world".encode("utf-8")).decode("utf-8")
    )

    arp = AzureResourcePermissions(
        installation, ws, create_autospec(AzureResources), create_autospec(ExternalLocations)
    )

    sp_crawler = create_autospec(AzureServicePrincipalCrawler)
    sp_crawler.snapshot.return_value = [
        AzureServicePrincipalInfo("app_secret1", "test_scope", "test_key", "tenant_id_1", "storage1"),
        AzureServicePrincipalInfo("app_secret2", "test_scope", "test_key", "tenant_id_1", "storage1"),
        AzureServicePrincipalInfo("app_secret3", "test_scope", "", "tenant_id_2", "storage1"),
        AzureServicePrincipalInfo("app_secret4", "", "", "tenant_id_2", "storage1"),
    ]

    return ServicePrincipalMigration(installation, ws, arp, sp_crawler, credential_manager)


def test_for_cli_not_prompts(ws, installation):
    ws.config.is_azure = True
    prompts = MockPrompts({"Have you reviewed the azure_storage_account_info.csv *": "No"})
    with pytest.raises(SystemExit):
        ServicePrincipalMigration.for_cli(ws, installation, prompts)


def test_for_cli(ws, installation):
    ws.config.is_azure = True
    ws.config.auth_type = "azure-cli"
    prompts = MockPrompts({"Have you reviewed the azure_storage_account_info.csv *": "Yes"})

    assert isinstance(ServicePrincipalMigration.for_cli(ws, installation, prompts), ServicePrincipalMigration)


@pytest.mark.parametrize(
    "secret_bytes_value, num_migrated",
    [
        (GetSecretResponse(value=base64.b64encode("hello world".encode("utf-8")).decode("utf-8")), 1),
        (GetSecretResponse(value=base64.b64encode("Olá, Mundo".encode("iso-8859-1")).decode("iso-8859-1")), 0),
    ],
)
def test_read_secret_value_decode(ws, sp_migration, secret_bytes_value, num_migrated):
    ws.secrets.get_secret.return_value = secret_bytes_value

    prompts = MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials*": "Yes"})
    assert len(sp_migration.run(prompts)) == num_migrated


def test_read_secret_value_none(ws, sp_migration):
    ws.secrets.get_secret.return_value = GetSecretResponse(value=None)
    prompts = MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials*": "Yes"})
    with pytest.raises(AssertionError):
        sp_migration.run(prompts)


def test_read_secret_read_exception(caplog, ws, sp_migration):
    caplog.set_level(logging.INFO)
    ws.secrets.get_secret.side_effect = ResourceDoesNotExist()

    prompts = MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials*": "Yes"})

    assert len(sp_migration.run(prompts)) == 0
    assert re.search(r"removed on the backend: .*", caplog.text)


def test_print_action_plan(caplog, ws, sp_migration):
    caplog.set_level(logging.INFO)
    ws.secrets.get_secret.return_value = GetSecretResponse(
        value=base64.b64encode("hello world".encode("utf-8")).decode("utf-8")
    )

    prompts = MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials*": "Yes"})

    sp_migration.run(prompts)

    log_pattern = r"Service Principal name: .* application_id: .* privilege .* on location .*"
    for msg in caplog.messages:
        if re.search(log_pattern, msg):
            assert True
            return
    assert False, "Action plan is not logged"


def test_run_without_confirmation(ws, sp_migration):
    ws.secrets.get_secret.return_value = GetSecretResponse(
        value=base64.b64encode("hello world".encode("utf-8")).decode("utf-8")
    )
    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "No",
        }
    )

    assert sp_migration.run(prompts) == []


def test_run(ws, installation, sp_migration):
    prompts = MockPrompts({"Above Azure Service Principals will be migrated to UC storage credentials*": "Yes"})

    sp_migration.run(prompts)
    installation.assert_file_written(
        "azure_service_principal_migration_result.csv",
        [
            {
                'application_id': 'app_secret1',
                'directory_id': 'directory_id_1',
                'name': 'principal_1',
                'validated_on': 'prefix1',
            }
        ],
    )
