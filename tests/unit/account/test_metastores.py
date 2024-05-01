from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import Workspace, WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import MetastoreInfo
from databricks.sdk.service.settings import DefaultNamespaceSetting, StringMessage

from databricks.labs.ucx.account.metastores import AccountMetastores


def test_show_all_metastores(acc_client, caplog):
    caplog.set_level("INFO")
    acc_client.metastores.list.return_value = [
        MetastoreInfo(name="metastore_usw", metastore_id="123", region="us-west-2"),
        MetastoreInfo(name="metastore_use", metastore_id="124", region="us-east-2"),
        MetastoreInfo(name="metastore_usc", metastore_id="125", region="us-central-1"),
    ]
    acc_client.workspaces.get.return_value = Workspace(workspace_id=123456, aws_region="us-west-2")
    account_metastores = AccountMetastores(acc_client)
    # no workspace id, should return all metastores
    account_metastores.show_all_metastores()
    assert "metastore_usw - 123" in caplog.messages
    assert "metastore_use - 124" in caplog.messages
    assert "metastore_usc - 125" in caplog.messages
    caplog.clear()
    # should only return usw metastore
    account_metastores.show_all_metastores("123456")
    assert "metastore_usw - 123" in caplog.messages
    # switch cloud, should only return use metastore
    caplog.clear()
    acc_client.config = Config(host="https://accounts.azuredatabricks.net", account_id="123", token="123")
    acc_client.workspaces.get.return_value = Workspace(workspace_id=123456, location="us-east-2")
    account_metastores.show_all_metastores("123456")
    assert "metastore_use - 124" in caplog.messages


def test_assign_metastore(acc_client):
    acc_client.metastores.list.return_value = [
        MetastoreInfo(name="metastore_usw_1", metastore_id="123", region="us-west-2"),
        MetastoreInfo(name="metastore_usw_2", metastore_id="124", region="us-west-2"),
        MetastoreInfo(name="metastore_usw_3", metastore_id="125", region="us-west-2"),
        MetastoreInfo(name="metastore_use_3", metastore_id="126", region="us-east-2"),
    ]
    acc_client.workspaces.list.return_value = [
        Workspace(workspace_name="foo", workspace_id=123456, aws_region="us-west-2"),
        Workspace(workspace_name="bar", workspace_id=123457, aws_region="us-east-2"),
    ]
    acc_client.workspaces.get.return_value = Workspace(workspace_id=123456, aws_region="us-west-2")
    ws = create_autospec(WorkspaceClient)
    acc_client.get_workspace_client.return_value = ws
    ws.settings.default_namespace.get.return_value = DefaultNamespaceSetting(
        etag="123", namespace=StringMessage("hive_metastore")
    )
    account_metastores = AccountMetastores(acc_client)
    prompts = MockPrompts({"Multiple metastores found, please select one*": "0", "Please select a workspace:*": "0"})

    # need to select a workspace - since it is alphabetically sorted, needs to pick workspace bar
    account_metastores.assign_metastore(prompts, "", "", "")
    acc_client.metastore_assignments.create.assert_called_with(123457, "123")

    # multiple metastores & default catalog name, need to choose one
    account_metastores.assign_metastore(prompts, "123456", "", "main")
    acc_client.metastore_assignments.create.assert_called_with(123456, "123")
    ws.settings.default_namespace.update.assert_called_with(
        allow_missing=True,
        field_mask="namespace.value",
        setting=DefaultNamespaceSetting(etag="123", namespace=StringMessage("main")),
    )

    # default catalog not found, still get etag
    ws.settings.default_namespace.get.side_effect = NotFound(details=[{"metadata": {"etag": "not_found"}}])
    account_metastores.assign_metastore(prompts, "123456", "", "main")
    acc_client.metastore_assignments.create.assert_called_with(123456, "123")
    ws.settings.default_namespace.update.assert_called_with(
        allow_missing=True,
        field_mask="namespace.value",
        setting=DefaultNamespaceSetting(etag="not_found", namespace=StringMessage("main")),
    )

    # only one metastore, should assign directly
    acc_client.workspaces.get.return_value = Workspace(workspace_id=123456, aws_region="us-east-2")
    account_metastores.assign_metastore(MockPrompts({}), "123456")
    acc_client.metastore_assignments.create.assert_called_with(123456, "126")

    # no metastore found, error
    acc_client.workspaces.get.return_value = Workspace(workspace_id=123456, aws_region="us-central-2")
    with pytest.raises(ValueError):
        account_metastores.assign_metastore(MockPrompts({}), "123456")
