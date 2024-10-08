import io
import os
from unittest.mock import create_autospec

import yaml

from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service import iam
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.sdk import AccountClient
from databricks.sdk.errors import PermissionDenied

from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import AccountInstaller
from . import mock_workspace_client

PRODUCT_INFO = ProductInfo.from_class(WorkspaceConfig)


def test_join_collection_prompt_no_join():
    ws = mock_workspace_client()
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_installer = AccountInstaller(account_client)
    prompts = MockPrompts(
        {
            r"Do you want to join the current.*": "no",
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection([123], True)
    account_client.workspaces.list.assert_not_called()


def test_join_collection_join_collection_account_admin():
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
            r"Do you want to join the current.*": "yes",
            r"Please select a workspace, the current installation.*": 1,
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection([123], True)
    ws.workspace.upload.assert_called()
    assert ws.workspace.upload.call_count == 2


def test_join_collection_join_collection_account_admin_workspace_id_not_collection_workspace_admin():
    ws = mock_workspace_client()
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_client.workspaces.list.side_effect = [
        PermissionDenied('access denied'),
    ]
    account_installer = AccountInstaller(account_client)
    prompts = MockPrompts(
        {
            r"Do you want to join the current.*": "yes",
            r"Please select a workspace, the current installation.*": 1,
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection([123], True)
    ws.workspace.upload.assert_not_called()


def test_join_collection_account_admin_workspace_id_not_installed_workspace_admin():
    ws = mock_workspace_client()
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_client.workspaces.list.side_effect = [
        [
            Workspace(workspace_id=123, deployment_name="test"),
            Workspace(workspace_id=456, deployment_name="test2"),
            Workspace(workspace_id=789, deployment_name="test3"),
        ]
    ]
    account_installer = AccountInstaller(account_client)

    prompts = MockPrompts(
        {
            r"Do you want to join the current.*": "yes",
            r"Please select a workspace, the current installation.*": 1,
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection([789, 678])
    ws.workspace.upload.assert_not_called()


def test_join_collection_join_existing_collection():
    ws = mock_workspace_client()

    account_client = create_autospec(AccountClient)
    account_client.workspaces.list.return_value = [
        Workspace(workspace_id=123, deployment_name="test"),
        Workspace(workspace_id=456, deployment_name="test2"),
        Workspace(workspace_id=789, deployment_name="test3"),
    ]
    account_client.get_workspace_client.return_value = ws
    account_installer = AccountInstaller(account_client)
    account_workspace = create_autospec(AccountWorkspaces)
    account_workspace.can_administer.return_value = True
    account_installer.replace(account_workspaces=account_workspace)
    account_installer.replace(
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection([123, 789])
    ws.workspace.upload.assert_called()
    assert ws.workspace.upload.call_count == 2


def test_get_workspaces_context_not_collection_admin(caplog):
    ws = mock_workspace_client()
    ws.current_user.me.side_effect = lambda: iam.User(user_name="not-admin@example.com")
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_installer = AccountInstaller(account_client)
    workspaces_ctx = account_installer.get_workspace_contexts(ws, True)
    assert len(workspaces_ctx) == 0
    assert 'User is not workspace admin of collection workspace 123' in caplog.text


def test_get_workspaces_context_empty_collection(caplog):
    ws = mock_workspace_client()
    download_yaml = {
        'config.yml': yaml.dump(
            {
                'version': 1,
                'inventory_database': 'ucx',
                'connect': {
                    'host': '...',
                    'token': '...',
                },
            }
        ),
        'workspaces.json': None,
    }
    ws.workspace.download.side_effect = lambda file_name: io.StringIO(download_yaml[os.path.basename(file_name)])
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_installer = AccountInstaller(account_client)
    workspaces_ctx = account_installer.get_workspace_contexts(ws, True)
    assert len(workspaces_ctx) == 0
    assert 'No collection info found in the workspace 123' in caplog.text


def test_get_workspaces_context_not_workspace_admin(caplog):
    ws = mock_workspace_client()
    ws.current_user.me.side_effect = [
        iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")]),
        iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")]),
        iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")]),
        iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="notadmin")]),
    ]
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_installer = AccountInstaller(account_client)
    workspaces_ctx = account_installer.get_workspace_contexts(ws, True)
    assert len(workspaces_ctx) == 0
    assert 'User is not workspace admin of workspace 456' in caplog.text


def test_get_workspaces_context():
    ws = mock_workspace_client()
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_installer = AccountInstaller(account_client)
    workspaces_ctx = account_installer.get_workspace_contexts(ws, True)
    assert len(workspaces_ctx) == 2
