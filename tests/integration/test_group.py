from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.managers.group import GroupLevel, GroupManager


def test_group_listing(ws: WorkspaceClient, make_ucx_group):
    ws_group, acc_group = make_ucx_group()
    manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    available_groups = manager._list_available_groups()
    assert ws_group.display_name in [g.display_name for g in available_groups.workspace]
    assert acc_group.display_name in [g.display_name for g in available_groups.account]


def test_id_validity(ws: WorkspaceClient, make_ucx_group):
    ws_group, acc_group = make_ucx_group()
    manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    assert ws_group.id == manager._get_group(ws_group.display_name, GroupLevel.WORKSPACE).id
    assert acc_group.id == manager._get_group(acc_group.display_name, GroupLevel.ACCOUNT).id
