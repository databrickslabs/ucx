from datetime import timedelta
from unittest.mock import create_autospec

from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import AccountClient
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import WorkspaceInstallation
from databricks.labs.ucx.installer.workflows import WorkflowsDeployment

PRODUCT_INFO = ProductInfo.from_class(WorkspaceConfig)


def workspace_installation_prepare(ws_patcher, installation_patcher, account_client, prompts):
    sql_backend = MockBackend()
    install_state = InstallState.from_installation(installation_patcher)
    wheels = PRODUCT_INFO.wheels(ws_patcher)
    workflows_installer = WorkflowsDeployment(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        installation_patcher,
        install_state,
        ws_patcher,
        wheels,
        PRODUCT_INFO,
        timedelta(seconds=1),
        [],
    )
    workspace_installation = WorkspaceInstallation(
        WorkspaceConfig(inventory_database="...", policy_id='123'),
        installation_patcher,
        install_state,
        sql_backend,
        ws_patcher,
        workflows_installer,
        prompts,
        PRODUCT_INFO,
        account_client,
    )
    return workspace_installation


def test_join_collection_prompt_no_join(ws, mock_installation):
    account_client = create_autospec(AccountClient)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "no",
            r".*": "",
        }
    )
    workspace_installation = workspace_installation_prepare(ws, mock_installation, account_client, prompts)
    workspace_installation.run()
    account_client.workspaces.list.assert_not_called()


def test_join_collection_no_sync_called(ws, mock_installation):
    account_client = create_autospec(AccountClient)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "yes",
            r".*": "",
        }
    )
    workspace_installation = workspace_installation_prepare(ws, mock_installation, account_client, prompts)
    workspace_installation.run()
    account_client.get_workspace_client.assert_not_called()


def test_join_collection_join_collection(ws, mock_installation):
    account_client = create_autospec(AccountClient)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "yes",
            r".*": "",
        }
    )
    workspace_installation = workspace_installation_prepare(ws, mock_installation, account_client, prompts)
    workspace_installation.run()
    account_client.get_workspace_client.assert_not_called()
