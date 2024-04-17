import base64
from unittest.mock import create_autospec

from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AzureServicePrincipal, StorageCredentialInfo

from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext


def test_replace_installation():
    ws = create_autospec(WorkspaceClient)
    ws.config.auth_type = 'azure-cli'
    ws.secrets.get_secret.return_value.value = base64.b64encode(b'1234').decode('utf-8')

    azure_service_principal = AzureServicePrincipal(
        directory_id="tenant", application_id="first-application-id", client_secret="secret"
    )
    storage_credential_info = StorageCredentialInfo(
        azure_service_principal=azure_service_principal,
        name="oneenv-adls",
        read_only=False,
    )
    ws.storage_credentials.create.return_value = storage_credential_info

    spn_info_rows = MockBackend.rows('application_id', 'secret_scope', 'secret_key', 'tenant_id', 'storage_account')

    mock_installation = MockInstallation(
        {
            'config.yml': {
                'inventory_database': 'some',
                'warehouse_id': 'other',
                'connect': {
                    'host': 'localhost',
                    'token': '1234',
                },
            },
            'azure_storage_account_info.csv': [
                {
                    'prefix': 'abfss://uctest@ziyuanqintest.dfs.core.windows.net/',
                    'client_id': "first-application-id",
                    'directory_id': 'tenant',
                    'principal': "oneenv-adls",
                    'privilege': "WRITE_FILES",
                    'type': "Application",
                },
                {
                    'prefix': 'abfss://ucx2@ziyuanqintest.dfs.core.windows.net/',
                    'client_id': "second-application-id",
                    'principal': "ziyuan-user-assigned-mi",
                    'privilege': "WRITE_FILES",
                    'type': "ManagedIdentity",
                },
            ],
        }
    )
    ctx = WorkspaceContext(ws).replace(
        is_azure=True,
        azure_subscription_id='foo',
        installation=mock_installation,
        sql_backend=MockBackend(
            rows={
                r'some.azure_service_principals': spn_info_rows[
                    ('first-application-id', 'foo', 'bar', 'tenant', 'ziyuanqintest'),
                    ('second-application-id', 'foo', 'bar', 'tenant', 'ziyuanqintest'),
                ]
            }
        ),
    )
    prompts = MockPrompts(
        {
            "Above Azure Service Principals will be migrated to UC storage credentials*": "Yes",
            "Please confirm to create an access connector for each storage account.": "No",
        }
    )
    ctx.service_principal_migration.run(prompts)

    ws.storage_credentials.create.assert_called_once()
    mock_installation.assert_file_written(
        'azure_service_principal_migration_result.csv',
        [
            {
                'name': 'oneenv-adls',
                'application_id': 'first-application-id',
                'validated_on': 'abfss://uctest@ziyuanqintest.dfs.core.windows.net/',
                'directory_id': 'tenant',
            }
        ],
    )
