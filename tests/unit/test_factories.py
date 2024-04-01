from unittest.mock import create_autospec

from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.factories import WorkspaceContext


def test_replace_installation():
    ws = create_autospec(WorkspaceClient)
    ws.config.auth_type = 'azure-cli'
    ctx = WorkspaceContext(ws)
    ctx.installation = MockInstallation(
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
            ],
        }
    )

    all_perms = ctx.azure_resource_permissions.load()
    assert len(all_perms) == 2
