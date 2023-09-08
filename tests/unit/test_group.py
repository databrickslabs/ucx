from unittest.mock import Mock, call

import pytest
from databricks.sdk.service.iam import Group, ResourceMeta

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.managers.group import GroupManager
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
    account_admins_group = Group(display_name="account admins", meta=ResourceMeta(resource_type="AccountGroup"))
    client.groups.list.return_value = [users_group, account_admins_group]

    group_conf = GroupsConfig(selected=[""])

    assert GroupManager(client, group_conf)._find_eligible_groups() == [users_group.display_name]


def test_if_only_account_groups_it_should_return_empty_value():
    client = Mock()
    users_group = Group(display_name="analysts", meta=ResourceMeta(resource_type="AccountGroup"))
    account_admins_group = Group(display_name="account admins", meta=ResourceMeta(resource_type="AccountGroup"))
    client.groups.list.return_value = [users_group, account_admins_group]

    group_conf = GroupsConfig(selected=[""])

    assert GroupManager(client, group_conf)._find_eligible_groups() == []


def test_backup_group_should_be_created_with_name_defined_in_conf():
    client = Mock()

    analysts_group = Group(display_name="analysts", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    client.groups.list.return_value = []

    analysts_group_backup = Group(
        display_name="dbr_backup_analysts_group_backup", meta=ResourceMeta(resource_type="WorkspaceGroup")
    )
    client.groups.create.return_value = analysts_group_backup

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

    group_conf = GroupsConfig(selected=[""], backup_group_prefix="dbr_backup_")

    assert (
        GroupManager(client, group_conf)._get_or_create_backup_group(
            "dbr_backup_analysts_group_backup", analysts_group_backup
        )
        == analysts_group_backup
    )


def test_prepare_groups_in_environment_with_one_group_in_conf_should_return_migrationgroupinfo_object():
    de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    def my_side_effect(filter, **kwargs):  # noqa: A002,ARG001
        if filter == "displayName eq 'de'":
            return [de_group]
        elif filter == "displayName eq 'dbr_backup_de'":
            return [backup_de_group]

    client = Mock()
    client.groups.list.side_effect = my_side_effect
    client.api_client.do.return_value = {"Resources": [de_group.as_dict()]}

    group_conf = GroupsConfig(selected=["de"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()

    group_info = MigrationGroupInfo(workspace=de_group, account=de_group, backup=backup_de_group)
    assert manager._migration_state.groups == [group_info]


def test_prepare_groups_in_environment_with_multiple_groups_in_conf_should_return_two_migrationgroupinfo_object():
    de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    ds_group = Group(display_name="ds", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup_ds_group = Group(display_name="dbr_backup_ds", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    client = Mock()

    def api_client_side_effect(method, path, query, **kwargs):  # noqa: ARG001
        if query == {
            "filter": "displayName eq 'ds'",
            "attributes": "id,displayName,meta,entitlements,roles,members",
            "excludedAttributes": None,
        }:
            return {"Resources": [ds_group.as_dict()]}
        elif query == {
            "filter": "displayName eq 'de'",
            "attributes": "id,displayName,meta,entitlements,roles,members",
            "excludedAttributes": None,
        }:
            return {"Resources": [de_group.as_dict()]}

    client.api_client.do.side_effect = api_client_side_effect

    def list_side_effect(filter, **kwargs):  # noqa: A002,ARG001
        if filter == "displayName eq 'de'":
            return [de_group]
        elif filter == "displayName eq 'ds'":
            return [ds_group]
        elif filter == "displayName eq 'dbr_backup_de'":
            return [backup_de_group]
        elif filter == "displayName eq 'dbr_backup_ds'":
            return [backup_ds_group]

    client.groups.list.side_effect = list_side_effect

    group_conf = GroupsConfig(selected=["de", "ds"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()

    de_group_info = MigrationGroupInfo(workspace=de_group, account=de_group, backup=backup_de_group)
    ds_group_info = MigrationGroupInfo(workspace=ds_group, account=ds_group, backup=backup_ds_group)

    assert compare(manager._migration_state.groups, [ds_group_info, de_group_info])


def test_prepare_groups_in_environment_should_throw_when_account_group_doesnt_exist():
    de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    client = Mock()
    client.api_client.do.return_value = {}
    client.groups.list.return_value = [de_group]

    group_conf = GroupsConfig(selected=["de"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)

    with pytest.raises(AssertionError) as e_info:
        manager.prepare_groups_in_environment()
    assert str(e_info.value) == "Group de not found on the account level"


def test_prepare_groups_in_environment_should_throw_when_workspace_group_doesnt_exist():
    client = Mock()
    client.api_client.do.return_value = {}
    client.groups.list.return_value = []

    group_conf = GroupsConfig(selected=["de"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)

    with pytest.raises(AssertionError) as e_info:
        manager.prepare_groups_in_environment()
    assert str(e_info.value) == "Group de not found on the workspace level"


def test_prepare_groups_in_environment_with_backup_group_not_created_should_create_it():
    de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    def groups_list_side_effect(filter, **kwargs):  # noqa: A002,ARG001
        if filter == "displayName eq 'de'":
            return [de_group]
        elif filter == "displayName eq 'dbr_backup_de'":
            return []
        elif filter == 'displayName ne "users" and displayName ne "admins" and displayName ne "account users"':
            return [de_group]

    client = Mock()
    client.groups.list.side_effect = groups_list_side_effect
    client.api_client.do.return_value = {"Resources": [de_group.as_dict()]}
    client.groups.create.return_value = backup_de_group

    group_conf = GroupsConfig(selected=["de"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()

    group_info = MigrationGroupInfo(workspace=de_group, account=de_group, backup=backup_de_group)
    assert manager._migration_state.groups == [group_info]


def test_prepare_groups_in_environment_with_conf_in_auto_mode_should_populate_migrationgroupinfo_object():
    client = Mock()

    de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    def my_side_effect(filter, **kwargs):  # noqa: A002,ARG001
        if filter == "displayName eq 'de'":
            return [de_group]
        elif filter == "displayName eq 'dbr_backup_de'":
            return [backup_de_group]
        elif filter == 'displayName ne "users" and displayName ne "admins" and displayName ne "account users"':
            return [de_group]

    client.groups.list.side_effect = my_side_effect
    client.api_client.do.return_value = {"Resources": [de_group.as_dict()]}

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()

    group_info = MigrationGroupInfo(workspace=de_group, account=de_group, backup=backup_de_group)
    assert manager._migration_state.groups == [group_info]


def test_prepare_groups_in_environment_with_conf_in_auto_mode_and_backup_group_exists_should_call_list_and_do():
    client = Mock()

    de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    def my_side_effect(filter, **kwargs):  # noqa: A002,ARG001
        if filter == "displayName eq 'de'":
            return [de_group]
        elif filter == "displayName eq 'dbr_backup_de'":
            return [backup_de_group]
        elif filter == 'displayName ne "users" and displayName ne "admins" and displayName ne "account users"':
            return [de_group]

    client.groups.list.side_effect = my_side_effect
    client.api_client.do.return_value = {"Resources": [de_group.as_dict()]}

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()

    assert client.mock_calls == [
        call.groups.list(
            attributes="displayName,meta",
            filter='displayName ne "users" and displayName ne "admins" and displayName ne "account users"',
        ),
        call.groups.list(attributes="id,displayName,meta,entitlements,roles,members", filter="displayName eq 'de'"),
        call.api_client.do(
            "GET",
            "/api/2.0/account/scim/v2/Groups",
            query={
                "filter": "displayName eq 'de'",
                "attributes": "id,displayName,meta,entitlements,roles,members",
                "excludedAttributes": None,
            },
        ),
        call.groups.list(
            filter="displayName eq 'dbr_backup_de'", attributes="id,displayName,meta,entitlements,roles,members"
        ),
    ]


def test_prepare_groups_in_environment_with_no_groups_in_conf_and_backup_group_exists_should_make_api_calls():
    client = Mock()

    de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    def my_side_effect(filter, **kwargs):  # noqa: A002,ARG001
        if filter == "displayName eq 'de'":
            return [de_group]
        elif filter == "displayName eq 'dbr_backup_de'":
            return []
        elif filter == 'displayName ne "users" and displayName ne "admins" and displayName ne "account users"':
            return [de_group]

    client.groups.list.side_effect = my_side_effect
    client.api_client.do.return_value = {"Resources": [de_group.as_dict()]}

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()

    assert client.mock_calls == [
        call.groups.list(
            attributes="displayName,meta",
            filter='displayName ne "users" and displayName ne "admins" and displayName ne "account users"',
        ),
        call.groups.list(attributes="id,displayName,meta,entitlements,roles,members", filter="displayName eq 'de'"),
        call.api_client.do(
            "GET",
            "/api/2.0/account/scim/v2/Groups",
            query={
                "filter": "displayName eq 'de'",
                "attributes": "id,displayName,meta,entitlements,roles,members",
                "excludedAttributes": None,
            },
        ),
        call.groups.list(
            filter="displayName eq 'dbr_backup_de'", attributes="id,displayName,meta,entitlements,roles,members"
        ),
        call.groups.create(
            display_name="dbr_backup_de",
            meta=ResourceMeta(resource_type="WorkspaceGroup"),
            entitlements=None,
            roles=None,
            members=None,
        ),
    ]


def test_replace_workspace_groups_with_account_groups_should_call_delete_and_do():
    client = Mock()

    test_workspace_id = 100
    de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"), id=test_workspace_id)
    backup_de_group = Group(display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    client.groups.list.return_value = [de_group]

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    manager = GroupManager(client, group_conf)

    group_info = MigrationGroupInfo(workspace=de_group, account=de_group, backup=backup_de_group)
    manager._migration_state.groups = [group_info]
    manager.replace_workspace_groups_with_account_groups()

    client.groups.delete.assert_called_with(test_workspace_id)
    client.api_client.do.assert_called_with(
        "PUT",
        f"/api/2.0/preview/permissionassignments/principals/{test_workspace_id}",
        data='{"permissions": ["USER"]}',
    )
