from unittest.mock import create_autospec, MagicMock

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AwsIamRole, AzureManagedIdentity, AzureServicePrincipal, StorageCredentialInfo

from databricks.labs.ucx.assessment.azure import StoragePermissionMapping
from databricks.labs.ucx.migration.azure_credentials import AzureServicePrincipalMigration


def test_list_storage_credentials():
    w = create_autospec(WorkspaceClient)

    w.storage_credentials.list.return_value = [
        StorageCredentialInfo(aws_iam_role=AwsIamRole(role_arn="arn:aws:iam::123456789012:role/example-role-name")),
        StorageCredentialInfo(azure_managed_identity=AzureManagedIdentity(access_connector_id="/subscriptions/.../providers/Microsoft.Databricks/...")),
        StorageCredentialInfo(azure_service_principal=AzureServicePrincipal(application_id="b6420590-5e1c-4426-8950-a94cbe9b6115",
                                                                            directory_id="62e43d7d-df53-4c64-86ed-c2c1a3ac60c3",
                                                                            client_secret="secret"))
    ]

    sp_migration = AzureServicePrincipalMigration(MagicMock(), w, MagicMock(), MagicMock())

    expected = {"b6420590-5e1c-4426-8950-a94cbe9b6115"}
    sp_migration._list_storage_credentials()

    assert expected == sp_migration._list_storage_credentials()


def test_sp_in_storage_credentials():
    storage_credentials_app_ids = {"no_match_id_1", "client_id_1", "client_id_2"}

    sp_no_match1 = StoragePermissionMapping(prefix="prefix3", client_id="client_id_3", principal="principal_3", privilege="READ_FILES", directory_id="directory_id_3")
    sp_match1 = StoragePermissionMapping(prefix="prefix1", client_id="client_id_1", principal="principal_1", privilege="WRITE_FILES", directory_id="directory_id_1")
    sp_match2 = StoragePermissionMapping(prefix="prefix2", client_id="client_id_2", principal="principal_2", privilege="WRITE_FILES", directory_id="directory_id_2")
    service_principals = [sp_no_match1, sp_match1, sp_match2]

    sp_migration = AzureServicePrincipalMigration(MagicMock(), MagicMock(), MagicMock(), MagicMock())

    filtered_sp_list = sp_migration._check_sp_in_storage_credentials(service_principals, storage_credentials_app_ids)

    assert filtered_sp_list == [sp_no_match1]


def test_sp_with_empty_storage_credentials():
    storage_credentials_app_ids = {}

    sp_no_match1 = StoragePermissionMapping(prefix="prefix3", client_id="client_id_3", principal="principal_3", privilege="READ_FILES", directory_id="directory_id_3")
    service_principals = [sp_no_match1]

    sp_migration = AzureServicePrincipalMigration(MagicMock(), MagicMock(), MagicMock(), MagicMock())

    filtered_sp_list = sp_migration._check_sp_in_storage_credentials(service_principals, storage_credentials_app_ids)

    assert filtered_sp_list == [sp_no_match1]