from datetime import timedelta
from unittest.mock import create_autospec
from databricks.sdk.service.provisioning import Workspace

from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import AccountClient
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import WorkspaceInstallation
from databricks.labs.ucx.installer.workflows import WorkflowsDeployment

PRODUCT_INFO = ProductInfo.from_class(WorkspaceConfig)


def workspace_installation_prepare(ws_patcher, account_client, prompts):
    sql_backend = MockBackend()
    mock_install = MockInstallation(
        {
            'config.yml': {
                'connect': {
                    'host': 'adb-9999999999999999.14.azuredatabricks.net',
                    'token': '...',
                },
                'inventory_database': 'ucx',
                'warehouse_id': 'abc',
                'installed_workspace_ids': [123, 456],
            },
        }
    )
    install_state = InstallState.from_installation(mock_install)
    wheels = PRODUCT_INFO.wheels(ws_patcher)
    workflows_installer = WorkflowsDeployment(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        mock_install,
        install_state,
        ws_patcher,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        [],
    )
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        mock_install,
        install_state,
        sql_backend,
        ws_patcher,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
        account_client,
    )
    return workspace_installation


def test_join_collection_prompt_no_join(ws):
    account_client = create_autospec(AccountClient)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "no",
            r".*": "",
        }
    )
    workspace_installation = workspace_installation_prepare(ws, account_client, prompts)
    workspace_installation.run()
    account_client.workspaces.list.assert_not_called()


def test_join_collection_no_sync_called(ws):
    account_client = create_autospec(AccountClient)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "yes",
            r".*": "",
        }
    )
    workspace_installation = workspace_installation_prepare(ws, account_client, prompts)
    workspace_installation.run()
    account_client.get_workspace_client.assert_not_called()


def test_join_collection_join_collection(ws, mocker):
    account_client = create_autospec(AccountClient)
    account_client.workspaces.list.return_value = [
        Workspace(workspace_id=123, deployment_name="test"),
        Workspace(workspace_id=456, deployment_name="test2"),
    ]
    account_client.get_workspace_client.return_value = ws
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "yes",
            r"Select the workspace to join current installation as a collection group": 1,
            r".*": "",
        }
    )
    ws.config.installed_workspace_ids = [123, 456]
    workspace_installation = workspace_installation_prepare(ws, account_client, prompts)
    workspace_installation.run()
    account_client.get_workspace_client.assert_not_called()
