import io
from unittest.mock import create_autospec

import yaml

from databricks.sdk.service.provisioning import Workspace

from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.sdk import AccountClient
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import AccountInstaller
from . import mock_workspace_client

PRODUCT_INFO = ProductInfo.from_class(WorkspaceConfig)


def test_join_collection_prompt_no_join():
    account_client = create_autospec(AccountClient)
    account_installer = AccountInstaller(account_client)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "no",
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection(123)
    account_client.workspaces.list.assert_not_called()


def test_join_collection_no_sync_called():
    account_client = create_autospec(AccountClient)
    account_installer = AccountInstaller(account_client)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "yes",
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection(123)
    account_client.workspaces.list.assert_called()
    account_client.get_workspace_client.assert_not_called()


def test_join_collection_join_collection_no_installation_id():
    ws = mock_workspace_client()
    download_yaml = yaml.dump(
        {
            'version': 1,
            'inventory_database': 'ucx_exists',
            'connect': {
                'host': '...',
                'token': '...',
            },
        }
    )
    ws.workspace.download.return_value = io.StringIO(download_yaml)
    account_client = create_autospec(AccountClient)
    account_client.workspaces.list.return_value = [
        Workspace(workspace_id=123, deployment_name="test"),
        Workspace(workspace_id=456, deployment_name="test2"),
    ]
    account_client.get_workspace_client.return_value = ws
    account_installer = AccountInstaller(account_client)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "yes",
            r"Please select a workspace, the current installation.*": 1,
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )

    account_installer.join_collection(456)
    ws.workspace.upload.assert_called()
    assert ws.workspace.upload.call_count == 1


def test_join_collection_join_collection():
    ws = mock_workspace_client()
    account_client = create_autospec(AccountClient)
    account_client.workspaces.list.return_value = [
        Workspace(workspace_id=123, deployment_name="test"),
        Workspace(workspace_id=456, deployment_name="test2"),
    ]
    account_client.get_workspace_client.return_value = ws
    account_installer = AccountInstaller(account_client)
    prompts = MockPrompts(
        {
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Open job overview.*": "no",
            r"Do you want to join the current.*": "yes",
            r"Please select a workspace, the current installation.*": 1,
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection(789)
    ws.workspace.upload.assert_called()
    assert ws.workspace.upload.call_count == 2
