from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.group import GroupLevel, GroupManager


def test_group_listing(ws: WorkspaceClient, make_ucx_group):
    ws_group, acc_group = make_ucx_group()
    manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    assert ws_group.display_name in [g.display_name for g in manager._workspace_groups]
    assert acc_group.display_name in [g.display_name for g in manager._account_groups]


def test_id_validity(ws: WorkspaceClient, make_ucx_group):
    ws_group, acc_group = make_ucx_group()
    manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    assert ws_group.id == manager._get_group(ws_group.display_name, GroupLevel.WORKSPACE).id
    assert acc_group.id == manager._get_group(acc_group.display_name, GroupLevel.ACCOUNT).id
