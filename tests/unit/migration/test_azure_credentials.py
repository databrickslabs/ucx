import logging
import pytest

from unittest.mock import MagicMock, create_autospec, Mock

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    InternalError,
    ResourceDoesNotExist
)
from databricks.sdk.service.catalog import (
    AwsIamRole,
    AzureManagedIdentity,
    AzureServicePrincipal,
    StorageCredentialInfo,
)
from databricks.sdk.service.workspace import GetSecretResponse

from databricks.labs.ucx.assessment.azure import StoragePermissionMapping, \
    AzureServicePrincipalCrawler, AzureServicePrincipalInfo
from databricks.labs.ucx.migration.azure_credentials import (
    AzureServicePrincipalMigration, ServicePrincipalMigrationInfo,
)
from tests.unit.framework.mocks import MockBackend


def test_list_storage_credentials():
    w = create_autospec(WorkspaceClient)

    w.storage_credentials.list.return_value = [
        StorageCredentialInfo(aws_iam_role=AwsIamRole(role_arn="arn:aws:iam::123456789012:role/example-role-name")),
        StorageCredentialInfo(
            azure_managed_identity=AzureManagedIdentity(
                access_connector_id="/subscriptions/.../providers/Microsoft.Databricks/..."
            )
        ),
        StorageCredentialInfo(
            azure_service_principal=AzureServicePrincipal(
                application_id="b6420590-5e1c-4426-8950-a94cbe9b6115",
                directory_id="62e43d7d-df53-4c64-86ed-c2c1a3ac60c3",
                client_secret="secret",
            )
        ),
    ]

    sp_migration = AzureServicePrincipalMigration(MagicMock(), w, MagicMock(), MagicMock())

    expected = {"b6420590-5e1c-4426-8950-a94cbe9b6115"}
    sp_migration._list_storage_credentials()

    assert expected == sp_migration._list_storage_credentials()


@pytest.mark.parametrize("secret_bytes_value, expected_return",
                         [(GetSecretResponse(value="aGVsbG8gd29ybGQ="), "hello world"),
                          (GetSecretResponse(value="T2zhLCBNdW5kbyE="), None)
                          ])
def test_read_secret_value_decode(secret_bytes_value, expected_return):
    w = create_autospec(WorkspaceClient)
    w.secrets.get_secret.return_value = secret_bytes_value

    sp_migration = AzureServicePrincipalMigration(MagicMock(), w, MagicMock(), MagicMock())
    assert sp_migration._read_databricks_secret("test_scope","test_key", "000") == expected_return


@pytest.mark.parametrize("exception, expected_log, expected_return",
                         [(ResourceDoesNotExist(), "Will not reuse this client_secret", None),
                          (InternalError(), "Will not reuse this client_secret", None)
                          ])
def test_read_secret_read_exception(caplog, exception, expected_log, expected_return):
    caplog.set_level(logging.INFO)
    w = create_autospec(WorkspaceClient)
    w.secrets.get_secret.side_effect = exception

    sp_migration = AzureServicePrincipalMigration(MagicMock(), w, MagicMock(), MagicMock())
    secret_value = sp_migration._read_databricks_secret("test_scope","test_key", "000")

    assert expected_log in caplog.text
    assert secret_value == expected_return


def test_fetch_client_secret():
    w = create_autospec(WorkspaceClient)
    w.secrets.get_secret.return_value = GetSecretResponse(value="aGVsbG8gd29ybGQ=")

    crawled_sp = [AzureServicePrincipalInfo("app_secret1", "test_scope", "test_key", "tenant_id_1", "storage1"),
                  AzureServicePrincipalInfo("app_secret2", "test_scope", "test_key", "tenant_id_1", "storage1"),
                  AzureServicePrincipalInfo("app_no_secret1", "", "", "tenant_id_1", "storage1"),
                  AzureServicePrincipalInfo("app_no_secret2", "test_scope", "", "tenant_id_1", "storage1"),]
    sp_crawler = AzureServicePrincipalCrawler(w, MockBackend(), "ucx")
    sp_crawler._try_fetch = Mock(return_value=crawled_sp)
    sp_crawler._crawl = Mock(return_value=crawled_sp)

    sp_to_be_checked = [StoragePermissionMapping(prefix="prefix1",client_id="app_secret1",principal="principal_1",privilege="WRITE_FILES",directory_id="directory_id_1"),
                       StoragePermissionMapping(prefix="prefix2",client_id="app_secret2",principal="principal_2",privilege="READ_FILES",directory_id="directory_id_1"),
                       StoragePermissionMapping(prefix="prefix3",client_id="app_no_secret1",principal="principal_3",privilege="WRITE_FILES",directory_id="directory_id_2"),
                       StoragePermissionMapping(prefix="prefix4",client_id="app_no_secret2",principal="principal_4",privilege="READ_FILES",directory_id="directory_id_2")]

    expected_sp_list = [ServicePrincipalMigrationInfo(StoragePermissionMapping(prefix="prefix1",client_id="app_secret1",principal="principal_1",privilege="WRITE_FILES",directory_id="directory_id_1"), "hello world"),
                        ServicePrincipalMigrationInfo(StoragePermissionMapping(prefix="prefix2",client_id="app_secret2",principal="principal_2",privilege="READ_FILES",directory_id="directory_id_1"), "hello world")]

    sp_migration = AzureServicePrincipalMigration(MagicMock(), w, MagicMock(), sp_crawler)
    filtered_sp_list = sp_migration._fetch_client_secret(sp_to_be_checked)

    assert filtered_sp_list == expected_sp_list


def test_print_action_plan(capsys):
    sp_list_with_secret = [ServicePrincipalMigrationInfo(StoragePermissionMapping(prefix="prefix1",client_id="app_secret1",principal="principal_1",privilege="WRITE_FILES",directory_id="directory_id_1"), "hello world")]
    sp_migration = AzureServicePrincipalMigration(MagicMock(), MagicMock(), MagicMock(), MagicMock())
    sp_migration._print_action_plan(sp_list_with_secret)

    expected_print = (f"Service Principal name: principal_1, "
                      f"application_id: app_secret1, "
                      f"privilege WRITE_FILES "
                      f"on location prefix1\n")
    assert expected_print == capsys.readouterr().out




