import io
import json
import os
from unittest.mock import create_autospec

import pytest
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
    account_installer.join_collection(
        [123], True
    )
    account_client.workspaces.list.assert_not_called()


def test_join_collection_join_collection_no_installation_id():
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
        'workspaces.json': json.dumps(
            [
                {'workspace_id': 123, 'deployment_name': 'test'},
                {'workspace_id': 456, 'deployment_name': 'test2'},
            ]
        ),
    }
    ws.workspace.download.side_effect = lambda file_name: io.StringIO(download_yaml[os.path.basename(file_name)])
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
    account_installer.join_collection([456], True)
    ws.workspace.upload.assert_called()
    assert ws.workspace.upload.call_count == 1


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
    account_installer.join_collection(
        [123],
    )
    ws.workspace.upload.assert_called()
    assert ws.workspace.upload.call_count == 3


def test_join_collection_join_collection_not_account_admin_no_workspace_id():
    ws = mock_workspace_client()
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_client.workspaces.list.side_effect = [
        [
            Workspace(workspace_id=123, deployment_name="test"),
            Workspace(workspace_id=456, deployment_name="test2"),
            Workspace(workspace_id=789, deployment_name="test3"),
        ],
        PermissionDenied('access denied'),
    ]
    account_installer = AccountInstaller(account_client)
    prompts = MockPrompts(
        {
            r"Do you want to join the current.*": "yes",
            r"Please select a workspace, the current installation.*": 1,
            r"Please enter, the workspace id to join as a collection.*": 0,
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection(
        [123],
    )
    ws.current_user.assert_not_called()


def test_join_collection_join_collection_not_account_admin_workspace_id_not_collection_workspace_admin():
    ws = mock_workspace_client()
    ws.current_user.me = lambda: iam.User(user_name="me@example.com")
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_client.workspaces.list.side_effect = [
        [
            Workspace(workspace_id=123, deployment_name="test"),
            Workspace(workspace_id=456, deployment_name="test2"),
            Workspace(workspace_id=789, deployment_name="test3"),
        ],
        PermissionDenied('access denied'),
    ]
    account_installer = AccountInstaller(account_client)
    prompts = MockPrompts(
        {
            r"Do you want to join the current.*": "yes",
            r"Please select a workspace, the current installation.*": 1,
            r"Please enter, the workspace id to join as a collection.*": 123,
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection([123])
    ws.workspace.upload.assert_not_called()


def test_join_collection_not_account_admin_workspace_id_not_installed_workspace_admin():
    ws = mock_workspace_client()
    account_client = create_autospec(AccountClient)
    ws.current_user.me.side_effect = [
        iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")]),
        iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")]),
        iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="admins")]),
        iam.User(user_name="me@example.com", groups=[iam.ComplexValue(display="notadmin")]),
    ]
    account_client.get_workspace_client.return_value = ws
    account_client.workspaces.list.side_effect = [
        [
            Workspace(workspace_id=123, deployment_name="test"),
            Workspace(workspace_id=456, deployment_name="test2"),
            Workspace(workspace_id=789, deployment_name="test3"),
        ],
        PermissionDenied('access denied'),
    ]
    account_installer = AccountInstaller(account_client)
    account_workspace = create_autospec(AccountWorkspaces)
    workspace_access = {123: True, 456: False, 789: True}
    account_workspace.can_administer.side_effect = lambda workspace: workspace_access[workspace.workspace_id]
    account_installer.replace(account_workspaces=account_workspace)
    prompts = MockPrompts(
        {
            r"Do you want to join the current.*": "yes",
            r"Please select a workspace, the current installation.*": 1,
            r"Please enter, the workspace id to join as a collection.*": 123,
            r".*": "",
        }
    )
    account_installer.replace(
        prompts=prompts,
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )
    account_installer.join_collection([789])
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
    account_installer.join_collection([123], 789)
    ws.workspace.upload.assert_called()
    assert ws.workspace.upload.call_count == 3


def test_join_collection_join_existing_collection_sync_not_upto_date():
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
    account_workspace = create_autospec(AccountWorkspaces)
    account_workspace.can_administer.return_value = True
    account_installer.replace(account_workspaces=account_workspace)
    account_installer.replace(
        product_info=ProductInfo.for_testing(WorkspaceConfig),
    )

    with pytest.raises(KeyError):
        account_installer.join_collection([789], 123)


def test_get_workspaces_context_not_collection_admin(caplog):
    ws = mock_workspace_client()
    ws.current_user.me = lambda: iam.User(user_name="me@example.com")
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_installer = AccountInstaller(account_client)
    workspaces_ctx = account_installer.get_workspace_contexts(123)
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
    workspaces_ctx = account_installer.get_workspace_contexts(123)
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
    workspaces_ctx = account_installer.get_workspace_contexts(123)
    assert len(workspaces_ctx) == 0
    assert 'User is not workspace admin of workspace 456' in caplog.text


def test_get_workspaces_context():
    ws = mock_workspace_client()
    account_client = create_autospec(AccountClient)
    account_client.get_workspace_client.return_value = ws
    account_installer = AccountInstaller(account_client)
    workspaces_ctx = account_installer.get_workspace_contexts(123)
    assert len(workspaces_ctx) == 2
