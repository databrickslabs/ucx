from unittest.mock import Mock

import pytest
from databricks.sdk.service.iam import Group, ResourceMeta

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.group import GroupManager
from databricks.labs.ucx.providers.groups_info import MigrationGroupInfo


def compare(s, t):
    t = list(t)  # make a mutable copy
    try:
        for elem in s:
            t.remove(elem)
    except ValueError:
        return False
    return not t


def test_account_groups_should_not_be_considered():
    client = Mock()
    users_group = Group(display_name="analysts", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    account_admins_group = Group(display_name="account admins", meta=ResourceMeta(resource_type="Group"))
    client.groups.list.return_value = [users_group, account_admins_group]
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_admins_group]],
    }

    group_conf = GroupsConfig(selected=[""])
    gm = GroupManager(client, group_conf)
    assert gm._list_workspace_groups() == [users_group]
    assert gm._list_account_groups() == [account_admins_group]


def test_if_only_account_groups_it_should_return_empty_value():
    client = Mock()
    Group(display_name="analysts", meta=ResourceMeta(resource_type="Group"))
    account_admins_group = Group(display_name="account admins", meta=ResourceMeta(resource_type="Group"))
    client.groups.list.return_value = []
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account_admins_group]],
    }
    group_conf = GroupsConfig(auto=True)
    gm = GroupManager(client, group_conf)
    assert gm._list_workspace_groups() == []
    assert gm._list_account_groups() == [account_admins_group]


def test_backup_group_should_be_created_with_name_defined_in_conf():
    client = Mock()

    analysts_group = Group(display_name="analysts", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    client.groups.list.return_value = []

    analysts_group_backup = Group(
        display_name="dbr_backup_analysts_group_backup", meta=ResourceMeta(resource_type="WorkspaceGroup")
    )
    client.groups.create.return_value = analysts_group_backup
    client.api_client.do.return_value = {}

    group_conf = GroupsConfig(selected=[""], backup_group_prefix="dbr_backup_")
    assert (
        GroupManager(client, group_conf)._get_or_create_backup_group("analysts", analysts_group)
        == analysts_group_backup
    )


def test_backup_group_should_not_be_created_if_already_exists():
    client = Mock()

    analysts_group_backup = Group(
        display_name="dbr_backup_analysts_group_backup", meta=ResourceMeta(resource_type="WorkspaceGroup")
    )
    client.groups.list.return_value = [analysts_group_backup]
    client.api_client.do.return_value = {}

    group_conf = GroupsConfig(auto=True, backup_group_prefix="dbr_backup_")
    assert (
        GroupManager(client, group_conf)._get_or_create_backup_group("analysts_group_backup", analysts_group_backup)
        == analysts_group_backup
    )


def test_prepare_groups_in_environment_with_one_group_in_conf_should_return_migrationgroupinfo_object():
    ws_de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    acc_de_group = Group(display_name="de", meta=ResourceMeta(resource_type="Group"))
    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    client = Mock()
    client.groups.list.return_value = [ws_de_group]
    client.groups.create.return_value = backup_de_group
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [acc_de_group]],
    }

    group_conf = GroupsConfig(selected=["de"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()

    group_info = MigrationGroupInfo(workspace=ws_de_group, account=acc_de_group, backup=backup_de_group)
    assert manager._migration_state.groups == [group_info]
    assert len(manager._workspace_groups) == 2  # created backup group should be added to the list


def test_prepare_groups_in_environment_with_multiple_groups_in_conf_should_return_two_migrationgroupinfo_object():
    ws_de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    ws_ds_group = Group(display_name="ds", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    acc_de_group = Group(display_name="de", meta=ResourceMeta(resource_type="Group"))
    acc_ds_group = Group(display_name="ds", meta=ResourceMeta(resource_type="Group"))

    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup_ds_group = Group(display_name="dbr_backup_ds", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    client = Mock()
    client.groups.list.return_value = iter([ws_de_group, ws_ds_group])
    client.groups.create.side_effect = [backup_de_group, backup_ds_group]
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [acc_de_group, acc_ds_group]],
    }
    group_conf = GroupsConfig(selected=["de", "ds"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()

    de_group_info = MigrationGroupInfo(workspace=ws_de_group, account=acc_de_group, backup=backup_de_group)
    ds_group_info = MigrationGroupInfo(workspace=ws_ds_group, account=acc_ds_group, backup=backup_ds_group)

    assert compare(manager._migration_state.groups, [ds_group_info, de_group_info])


def test_prepare_groups_in_environment_should_throw_when_account_group_doesnt_exist():
    de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    client = Mock()
    client.api_client.do.return_value = {}
    client.groups.list.return_value = [de_group]
    client.api_client.do.return_value = {}

    group_conf = GroupsConfig(selected=["de"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)

    with pytest.raises(AssertionError) as e_info:
        manager.prepare_groups_in_environment()
    assert str(e_info.value) == "Group de not found on the account level"


def test_prepare_groups_in_environment_should_throw_when_workspace_group_doesnt_exist():
    client = Mock()
    client.api_client.do.side_effect = [{}, {}]
    client.groups.list.return_value = []

    group_conf = GroupsConfig(selected=["de"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)
    with pytest.raises(AssertionError) as e_info:
        manager.prepare_groups_in_environment()
        assert str(e_info.value) == "Group de not found on the workspace level"


def test_prepare_groups_in_environment_with_backup_group_not_created_should_create_it():
    ws_de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    acc_de_group = Group(display_name="de", meta=ResourceMeta(resource_type="Group"))

    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    client = Mock()
    client.groups.list.return_value = iter([ws_de_group])
    client.groups.create.return_value = backup_de_group
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [acc_de_group]],
    }

    group_conf = GroupsConfig(selected=["de"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()
    group_info = MigrationGroupInfo(workspace=ws_de_group, account=acc_de_group, backup=backup_de_group)
    assert manager._migration_state.groups == [group_info]


def test_prepare_groups_in_environment_with_conf_in_auto_mode_should_populate_migrationgroupinfo_object():
    client = Mock()

    ws_de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    acc_de_group = Group(display_name="de", meta=ResourceMeta(resource_type="Group"))

    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    client.groups.list.return_value = iter([ws_de_group])
    client.groups.create.return_value = backup_de_group
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [acc_de_group]],
    }

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()

    group_info = MigrationGroupInfo(workspace=ws_de_group, account=acc_de_group, backup=backup_de_group)
    assert manager._migration_state.groups == [group_info]


def test_replace_workspace_groups_with_account_groups_should_call_delete_and_do():
    client = Mock()

    test_ws_group_id = "100"
    test_acc_group_id = "200"
    ws_de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"), id=test_ws_group_id)
    acc_de_group = Group(display_name="de", meta=ResourceMeta(resource_type="Group"), id=test_acc_group_id)

    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    client.groups.list.return_value = [ws_de_group]
    client.api_client.do.side_effect = [
        {
            "Resources": [g.as_dict() for g in [acc_de_group]],
        },
        {},
    ]

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    manager = GroupManager(client, group_conf)

    group_info = MigrationGroupInfo(workspace=ws_de_group, account=acc_de_group, backup=backup_de_group)
    manager._migration_state.groups = [group_info]
    manager.replace_workspace_groups_with_account_groups()

    client.groups.delete.assert_called_with(test_ws_group_id)
    client.api_client.do.assert_called_with(
        "PUT",
        f"/api/2.0/preview/permissionassignments/principals/{test_acc_group_id}",
        data='{"permissions": ["USER"]}',
    )


def test_delete_backup_groups():
    client = Mock()

    test_ws_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    test_acc_group = Group(display_name="de", meta=ResourceMeta(resource_type="Group"))
    backup_group_id = "100"
    client.groups.list.return_value = [test_ws_group]
    client.groups.create.return_value = Group(
        display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"), id=backup_group_id
    )
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [test_acc_group]],
    }

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", selected=["de"])
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()
    manager.delete_backup_groups()
    client.groups.delete.assert_called_with(id=backup_group_id)
