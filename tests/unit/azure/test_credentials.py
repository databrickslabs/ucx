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
    AwsIamRoleResponse,
    AzureManagedIdentityResponse,
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
    StorageCredentialValidationResult,
)
from databricks.labs.ucx.azure.resources import (
    AzureResource,
    AzureResources,
    StorageAccount,
    AccessConnector,
)
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation, ExternalLocations
from tests.unit import DEFAULT_CONFIG


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


def side_effect_create_storage_credential(
    name, comment, read_only, azure_service_principal=None, azure_managed_identity=None
):
    return StorageCredentialInfo(
        name=name,
        comment=comment,
        read_only=read_only,
        azure_service_principal=azure_service_principal,
        azure_managed_identity=azure_managed_identity,
    )


def side_effect_validate_storage_credential(storage_credential_name, url, read_only):
    _ = url
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
def credential_manager():
    ws = create_autospec(WorkspaceClient)
    ws.storage_credentials.list.return_value = [
        StorageCredentialInfo(aws_iam_role=AwsIamRoleResponse("arn:aws:iam::123456789012:role/example-role-name")),
        StorageCredentialInfo(
            azure_managed_identity=AzureManagedIdentityResponse("/subscriptions/.../providers/Microsoft.Databricks/...")
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


def test_storage_credential_validation_result_from_storage_credential_info_service_principal():
    """Verify the SP field to be present in the result"""
    azure_service_principal = AzureServicePrincipal("directory_id", "application_id", "client_secret")
    storage_credential_info = StorageCredentialInfo(
        name="test",
        azure_service_principal=azure_service_principal,
        read_only=True,
    )
    validated_on = "abfss://container@storageaccount.dfs.core.windows.net"
    failures = ["failure"]

    validation_result = StorageCredentialValidationResult.from_storage_credential_info(
        storage_credential_info, validated_on, failures
    )

    assert validation_result.name == storage_credential_info.name
    assert validation_result.application_id == azure_service_principal.application_id
    assert validation_result.read_only == storage_credential_info.read_only
    assert validation_result.validated_on == validated_on
    assert validation_result.directory_id == azure_service_principal.directory_id
    assert validation_result.failures == failures


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
            directory_id="directory_id_1",
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
            directory_id="directory_id_1",
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
    azure_service_principal = AzureServicePrincipal(directory_id="test", application_id="test", client_secret="secret")
    storage_credential_info = StorageCredentialInfo(
        azure_service_principal=azure_service_principal,
        name="storage_credential_info",
        read_only=False,
    )

    # validate normal storage credential
    validation = credential_manager.validate(storage_credential_info, "prefix")
    assert validation.read_only is False
    assert validation.name == storage_credential_info.name
    assert not validation.failures


def test_validate_read_only_storage_credentials(credential_manager):
    azure_service_principal = AzureServicePrincipal(directory_id="test", application_id="test", client_secret="secret")
    storage_credential_info = StorageCredentialInfo(
        azure_service_principal=azure_service_principal,
        name="storage_credential_info",
        read_only=True,
    )

    # validate read-only storage credential
    validation = credential_manager.validate(storage_credential_info, "prefix")
    assert validation.read_only is True
    assert validation.name == storage_credential_info.name
    assert not validation.failures


def test_validate_storage_credentials_overlap_location(credential_manager):
    azure_service_principal = AzureServicePrincipal(directory_id="test", application_id="test", client_secret="secret")
    storage_credential_info = StorageCredentialInfo(
        azure_service_principal=azure_service_principal,
        name="overlap",
        read_only=True,
    )

    # prefix used for validation overlaps with existing external location will raise InvalidParameterValue
    # assert InvalidParameterValue is handled
    validation = credential_manager.validate(storage_credential_info, "prefix")
    assert validation.failures == [
        "The validation is skipped because an existing external location overlaps with the location used for validation."
    ]


def test_validate_storage_credentials_non_response(credential_manager):
    azure_service_principal = AzureServicePrincipal(directory_id="test", application_id="test", client_secret="secret")
    storage_credential_info = StorageCredentialInfo(
        azure_service_principal=azure_service_principal,
        name="none",
        read_only=True,
    )

    validation = credential_manager.validate(storage_credential_info, "prefix")
    assert validation.failures == ["Validation returned no results."]


def test_validate_storage_credentials_failed_operation(credential_manager):
    azure_service_principal = AzureServicePrincipal(directory_id="test", application_id="test", client_secret="secret")
    storage_credential_info = StorageCredentialInfo(
        azure_service_principal=azure_service_principal,
        name="fail",
        read_only=True,
    )

    validation = credential_manager.validate(storage_credential_info, "prefix")
    assert validation.failures == ["LIST validation failed with message: fail"]


@pytest.fixture
def sp_migration(installation, credential_manager):
    ws = create_autospec(WorkspaceClient)
    ws.secrets.get_secret.return_value = GetSecretResponse(
        value=base64.b64encode("hello world".encode("utf-8")).decode("utf-8")
    )

    storage_account = StorageAccount(
        id=AzureResource("/subscriptions/test/providers/Microsoft.Storage/storageAccount/labsazurethings"),
        name="labsazurethings",
        location="westeu",
        default_network_action="Allow",
    )
    azurerm = create_autospec(AzureResources)
    azurerm.storage_accounts.return_value = [storage_account]

    access_connector_id = AzureResource(
        "/subscriptions/test/resourceGroups/rg-test/providers/Microsoft.Databricks/accessConnectors/ac-test"
    )
    azurerm.create_or_update_access_connector.return_value = AccessConnector(
        id=access_connector_id,
        name=f"ac-{storage_account.name}",
        location=storage_account.location,
        provisioning_state="Succeeded",
        identity_type="SystemAssigned",
        principal_id="test",
        tenant_id="test",
    )

    external_location = ExternalLocation(f"abfss://things@{storage_account.name}.dfs.core.windows.net/folder1", 1)
    external_locations = create_autospec(ExternalLocations)
    external_locations.snapshot.return_value = [external_location]

    arp = AzureResourcePermissions(installation, ws, azurerm, external_locations)

    sp_crawler = create_autospec(AzureServicePrincipalCrawler)
    arp = AzureResourcePermissions(installation, ws, azurerm, external_locations)
    sp_crawler.snapshot.return_value = [
        AzureServicePrincipalInfo("app_secret1", "test_scope", "test_key", "tenant_id_1", "storage1"),
        AzureServicePrincipalInfo("app_secret2", "test_scope", "test_key", "tenant_id_1", "storage1"),
        AzureServicePrincipalInfo("app_secret3", "test_scope", "", "tenant_id_2", "storage1"),
        AzureServicePrincipalInfo("app_secret4", "", "", "tenant_id_2", "storage1"),
    ]

    return ServicePrincipalMigration(installation, ws, arp, sp_crawler, credential_manager)


@pytest.mark.parametrize(
    "secret_bytes_value, num_migrated",
    [
        (GetSecretResponse(value=base64.b64encode("hello world".encode("utf-8")).decode("utf-8")), 1),
        (GetSecretResponse(value=base64.b64encode("Olá, Mundo".encode("iso-8859-1")).decode("iso-8859-1")), 0),
    ],
)
def test_read_secret_value_decode(sp_migration, secret_bytes_value, num_migrated):
    # due to abuse of fixtures and the way fixtures are shared in PyTest,
    # we need to access the protected attribute to keep the test small.
    # this test also reveals a design flaw in test code and perhaps in
    # the code under test as well.
    # pylint: disable-next=protected-access
    sp_migration._ws.secrets.get_secret.return_value = secret_bytes_value

    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "Yes",
            r"\[RECOMMENDED\] Please confirm to create an access connector*": "No",
        }
    )
    assert len(sp_migration.run(prompts)) == num_migrated


def test_read_secret_value_none(sp_migration):
    # due to abuse of fixtures and the way fixtures are shared in PyTest,
    # we need to access the protected attribute to keep the test small.
    # this test also reveals a design flaw in test code and perhaps in
    # the code under test as well.
    # pylint: disable-next=protected-access
    sp_migration._ws.secrets.get_secret.return_value = GetSecretResponse(value=None)
    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "Yes",
            r"\[RECOMMENDED\] Please confirm to create an access connector*": "No",
        }
    )
    with pytest.raises(AssertionError):
        sp_migration.run(prompts)


def test_read_secret_read_exception(caplog, sp_migration):
    caplog.set_level(logging.INFO)
    # due to abuse of fixtures and the way fixtures are shared in PyTest,
    # we need to access the protected attribute to keep the test small.
    # this test also reveals a design flaw in test code and perhaps in
    # the code under test as well.
    # pylint: disable-next=protected-access
    sp_migration._ws.secrets.get_secret.side_effect = ResourceDoesNotExist()

    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "Yes",
            r"\[RECOMMENDED\] Please confirm to create an access connector*": "No",
        }
    )

    assert len(sp_migration.run(prompts)) == 0
    assert re.search(r"removed on the backend: .*", caplog.text)


def test_print_action_plan(caplog, sp_migration):
    caplog.set_level(logging.INFO)
    ws = create_autospec(WorkspaceClient)
    ws.secrets.get_secret.return_value = GetSecretResponse(
        value=base64.b64encode("hello world".encode("utf-8")).decode("utf-8")
    )

    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "Yes",
            r"\[RECOMMENDED\] Please confirm to create an access connector*": "No",
        }
    )

    sp_migration.run(prompts)

    log_pattern = r"Service Principal name: .* application_id: .* privilege .* on location .*"
    for msg in caplog.messages:
        if re.search(log_pattern, msg):
            assert True
            return
    assert False, "Action plan is not logged"


def test_run_without_confirmation(sp_migration):
    ws = create_autospec(WorkspaceClient)
    ws.secrets.get_secret.return_value = GetSecretResponse(
        value=base64.b64encode("hello world".encode("utf-8")).decode("utf-8")
    )
    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "No",
            r"\[RECOMMENDED\] Please confirm to create an access connector*": "No",
        }
    )

    assert sp_migration.run(prompts) == []


def test_run(installation, sp_migration):
    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "Yes",
            r"\[RECOMMENDED\] Please confirm to create an access connector*": "No",
        }
    )

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


def test_run_warning_non_allow_network_configuration(installation, sp_migration, caplog):
    """The user should be warned when a network configuration is not 'Allow'"""
    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "Yes",
            r"\[RECOMMENDED\] Please confirm to create an access connector*": "No",
        }
    )

    expected_messages = (
        "At least one Azure Service Principal accesses a storage account with non-Allow default network",
        "Service principal 'principal_1' accesses storage account 'prefix1' with non-Allow network configuration",
    )

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx"):
        sp_migration.run(prompts)

    for expected_message in expected_messages:
        assert any(expected_message in message for message in caplog.messages), f"Message not logged {expected_message}"


def test_create_access_connectors_for_storage_accounts(sp_migration):
    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "No",
            r"\[RECOMMENDED\] Please confirm to create an access connector*": "Yes",
        }
    )

    validation_results = sp_migration.run(prompts)

    assert len(validation_results) == 1
    assert validation_results[0].name.startswith("ac")
    assert validation_results[0].failures is None
