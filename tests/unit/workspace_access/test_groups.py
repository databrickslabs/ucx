import json
from unittest.mock import MagicMock, Mock

import pytest
from databricks.sdk.service import iam
from databricks.sdk.service.iam import ComplexValue, Group, ResourceMeta

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.workspace_access.groups import (
    GroupManager,
    GroupMigrationState,
    MigrationGroupInfo,
    MigrationGroupInfoMock,
)
from databricks.labs.ucx.workspace_access.scim import Permissions, ScimSupport
from tests.unit.framework.mocks import MockBackend


def test_scim_crawler():
    ws = MagicMock()
    ws.groups.list.return_value = [
        iam.Group(
            id="1",
            display_name="group1",
            roles=[],  # verify that empty roles and entitlements are not returned
        ),
        iam.Group(
            id="2",
            display_name="group2",
            roles=[iam.ComplexValue(value="role1")],
            entitlements=[iam.ComplexValue(value="entitlement1")],
        ),
        iam.Group(
            id="3",
            display_name="group3",
            roles=[iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")],
            entitlements=[],
        ),
    ]
    sup = ScimSupport(ws=ws)
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 3
    ws.groups.list.assert_called_once()
    for task in tasks:
        item = task()
        if item.object_id == "1":
            assert item is None
        else:
            assert item.object_id in ["2", "3"]
            assert item.object_type in ["roles", "entitlements"]
            assert item.raw is not None


@pytest.mark.parametrize(
    "item",
    [
        Permissions(
            object_id="test-ws",
            object_type="roles",
            raw=json.dumps(
                [
                    iam.ComplexValue(value="role1").as_dict(),
                    iam.ComplexValue(value="role2").as_dict(),
                ]
            ),
        ),
        Permissions(
            object_id="test-ws",
            object_type="entitlements",
            raw=json.dumps(
                [
                    iam.ComplexValue(value="sql-access").as_dict(),
                    iam.ComplexValue(value="workspace-admin").as_dict(),
                ]
            ),
        ),
    ],
    ids=["roles", "entitlements"],
)
def test_scim_apply(item, migration_state):
    ws = MagicMock()
    value_payload: list[dict] = json.loads(item.raw)
    ws.groups.get.return_value = Group(
        **{"id": "test-backup", item.object_type: [iam.ComplexValue.from_dict(value) for value in value_payload]}
    )
    sup = ScimSupport(ws=ws)

    apply_to_backup_task = sup.get_apply_task(item, migration_state, "backup")
    apply_to_backup_task()

    ws.groups.patch.assert_called_once_with(
        id="test-backup",
        operations=[iam.Patch(op=iam.PatchOp.ADD, path=item.object_type, value=value_payload)],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )

    apply_to_account_task = sup.get_apply_task(item, migration_state, "account")
    apply_to_account_task()
    ws.groups.patch.assert_called_with(
        id="test-acc",
        operations=[iam.Patch(op=iam.PatchOp.ADD, path=item.object_type, value=value_payload)],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


def test_no_group_in_migration_state(migration_state):
    ws = MagicMock()
    sup = ScimSupport(ws=ws)
    sample_permissions = [iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")]
    item = Permissions(
        object_id="test-non-existent",
        object_type="roles",
        raw=json.dumps([p.as_dict() for p in sample_permissions]),
    )
    task = sup.get_apply_task(item, migration_state, "backup")
    assert task is None


def test_non_relevant(migration_state):
    ws = MagicMock()
    sup = ScimSupport(ws=ws)
    sample_permissions = [iam.ComplexValue(value="role1")]
    relevant_item = Permissions(
        object_id="test-ws",
        object_type="roles",
        raw=json.dumps([p.as_dict() for p in sample_permissions]),
    )
    irrelevant_item = Permissions(
        object_id="something-non-relevant",
        object_type="roles",
        raw=json.dumps([p.as_dict() for p in sample_permissions]),
    )
    assert sup._is_item_relevant(relevant_item, migration_state)
    assert not sup._is_item_relevant(irrelevant_item, migration_state)


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
    assert gm._list_account_groups(client) == [account_admins_group]


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
    assert gm._list_account_groups(client) == [account_admins_group]


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


def test_prepare_groups_in_environment_should_not_throw_when_account_group_doesnt_exist():
    de_group = Group(display_name="de", meta=ResourceMeta(resource_type="WorkspaceGroup"))

    client = Mock()
    client.api_client.do.return_value = {}
    client.groups.list.return_value = [de_group]
    client.api_client.do.return_value = {}

    group_conf = GroupsConfig(selected=["de"], backup_group_prefix="dbr_backup_")
    manager = GroupManager(client, group_conf)

    manager.prepare_groups_in_environment()
    assert len(manager.migration_state.groups) == 0


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


def test_prepare_groups_in_environment_with_no_groups():
    client = Mock()
    client.groups.list.return_value = iter([])
    client.api_client.do.return_value = {
        "Resources": [],
    }

    group_conf = GroupsConfig(auto=True)
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()
    assert not manager.has_groups()


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

    state = GroupMigrationState()
    state.add(ws_group=ws_de_group, acc_group=acc_de_group, backup_group=backup_de_group)
    manager.replace_workspace_groups_with_account_groups(state)

    client.groups.delete.assert_called_with(id=test_ws_group_id)
    client.api_client.do.assert_called_with(
        "PUT",
        f"/api/2.0/preview/permissionassignments/principals/{test_acc_group_id}",
        data='{"permissions": ["USER"]}',
    )


def test_system_groups():
    client = Mock()
    test_ws_group = Group(display_name="admins", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    test_acc_group = Group(display_name="admins", meta=ResourceMeta(resource_type="Group"))
    backup_group_id = "100"
    client.groups.list.return_value = [test_ws_group]
    client.groups.create.return_value = Group(
        display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"), id=backup_group_id
    )
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [test_acc_group]],
    }

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", selected=["admins"])
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()
    assert len(manager._migration_state.groups) == 0


def test_workspace_only_groups():
    client = Mock()
    test_ws_group = Group(display_name="ws_group", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    test_acc_group = Group(display_name="acc_group", meta=ResourceMeta(resource_type="Group"))
    backup_group_id = "100"
    client.groups.list.return_value = [test_ws_group, test_acc_group]
    client.groups.create.return_value = Group(
        display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"), id=backup_group_id
    )
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [test_acc_group]],
    }

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", selected=["ws_group"])
    manager = GroupManager(client, group_conf)
    manager.prepare_groups_in_environment()
    assert len(manager._migration_state.groups) == 0


def test_delete_backup_groups():
    client = Mock()

    backup_group_id = "100"
    ws_group = Group(display_name="de", meta=ResourceMeta(resource_type="Group"))
    test_ws_backup_group = Group(
        display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"), id=backup_group_id
    )

    client.groups.list.return_value = [ws_group, test_ws_backup_group]

    test_acc_group = Group(display_name="de", meta=ResourceMeta(resource_type="Group"))
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [test_acc_group]],
    }

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    manager = GroupManager(client, group_conf)
    manager.delete_original_workspace_groups()
    client.groups.delete.assert_called_with(id=backup_group_id)


def test_delete_selected_backup_groups():
    client = Mock()

    backup_group_id = "100"
    ws_group = Group(display_name="de", meta=ResourceMeta(resource_type="Group"))
    test_ws_backup_group = Group(
        display_name="dbr_backup_de", meta=ResourceMeta(resource_type="WorkspaceGroup"), id=backup_group_id
    )

    ws_group_to_skip = Group(display_name="de2", meta=ResourceMeta(resource_type="Group"))
    test_ws_backup_group_to_skip = Group(
        display_name="dbr_backup_de2", meta=ResourceMeta(resource_type="WorkspaceGroup"), id="1"
    )

    client.groups.list.return_value = [ws_group, test_ws_backup_group, ws_group_to_skip, test_ws_backup_group_to_skip]

    test_acc_group = Group(display_name="de", meta=ResourceMeta(resource_type="Group"))
    test_acc_group_to_skip = Group(display_name="de2", meta=ResourceMeta(resource_type="Group"))
    client.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [test_acc_group, test_acc_group_to_skip]],
    }

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", selected=["de"])
    manager = GroupManager(client, group_conf)
    manager.delete_original_workspace_groups()
    client.groups.delete.assert_called_with(id=backup_group_id)


def test_migration_state_should_be_saved_with_proper_values():
    workspace = Group(
        display_name="workspace", entitlements=[ComplexValue(display="entitlements", value="allow-cluster-create")]
    )
    backup = Group(display_name="db-temp-workspace", meta=ResourceMeta("test"))
    account = Group(display_name="account")
    schema = "test_schema"

    state = GroupMigrationState()
    state.add(workspace, backup, account)
    backend = MockBackend()

    state.persist_migration_state(backend, schema)
    rows = backend.rows_written_for(f"hive_metastore.{schema}.migration_state", "append")
    assert rows == [
        MigrationGroupInfoMock(
            workspace='{"displayName": "workspace", "entitlements": ['
            '{"display": "entitlements", "value": "allow-cluster-create"}'
            ']}',
            backup='{"displayName": "db-temp-workspace", "meta": {"resourceType": "test"}}',
            account='{"displayName": "account"}',
        )
    ]


def test_migration_state_should_be_saved_without_schema():
    account = Group(display_name="account", schemas=[iam.GroupSchema.URN_IETF_PARAMS_SCIM_SCHEMAS_CORE_2_0_GROUP])
    schema = "test_schema"

    state = GroupMigrationState()
    state.add(None, None, account)
    backend = MockBackend()

    state.persist_migration_state(backend, schema)
    rows = backend.rows_written_for(f"hive_metastore.{schema}.migration_state", "append")
    assert rows == [MigrationGroupInfoMock(workspace=None, backup=None, account='{"displayName": "account"}')]


def test_migration_state_should_not_filter_any_rows():
    schema = "test_schema"
    workspace = Group("workspace")
    backup = Group("db-temp-workspace")
    account = Group("workspace")

    state = GroupMigrationState()
    state.add(workspace, backup, account)
    state.add(workspace, backup, None)
    state.add(workspace, None, account)
    state.add(None, None, account)
    state.add(None, None, None)

    backend = MockBackend()
    state.persist_migration_state(backend, schema)
    rows = backend.rows_written_for(f"hive_metastore.{schema}.migration_state", "append")

    assert len(rows) == 5


def test_fetch_migration_state_should_return_all():
    schema = "test_schema"

    rows = {
        "SELECT": [
            (
                '{"displayName": "workspace", "entitlements": ['
                '{"display": "entitlements", "value": "allow-cluster-create"}'
                ']}',
                '{"displayName": "db-temp-workspace", "meta": {"resourceType": "test"}}',
                '{"displayName": "account"}',
            ),
        ]
    }

    backend = MockBackend(rows=rows)
    rows = GroupMigrationState().fetch_migration_state(backend, schema)

    state = GroupMigrationState()
    state.add(
        ws_group=Group(
            display_name="workspace", entitlements=[ComplexValue(display="entitlements", value="allow-cluster-create")]
        ),
        backup_group=Group(display_name="db-temp-workspace", meta=ResourceMeta(resource_type="test")),
        acc_group=Group(display_name="account"),
    )

    assert rows.groups == state.groups


def test_fetch_all():
    ws = MagicMock()

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    workspace = Group(display_name="data_engs", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup = Group(display_name="dbr_backup_data_engs", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    account = Group(display_name="data_engs", meta=ResourceMeta(resource_type="Group"))
    remote_state = GroupMigrationState()
    remote_state.add(workspace, backup, account)

    ws.groups.list.return_value = [backup, account]
    ws.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account]],
    }

    manager = GroupManager(ws, group_conf)
    new_state = manager.prepare_apply_permissions_to_account_groups(ws, remote_state, group_conf.backup_group_prefix)

    assert len(new_state) == 1


def test_group_present_in_state_but_not_in_workspace():
    ws = MagicMock()

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    workspace = Group(display_name="data_engs", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup = Group(display_name="dbr_backup_data_engs", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    account = Group(display_name="data_engs", meta=ResourceMeta(resource_type="Group"))
    remote_state = GroupMigrationState()
    remote_state.add(workspace, backup, account)

    ws.groups.list.return_value = [backup]
    ws.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account]],
    }

    manager = GroupManager(ws, group_conf)
    new_state = manager.prepare_apply_permissions_to_account_groups(ws, remote_state, group_conf.backup_group_prefix)

    assert len(new_state) == 0


def test_group_not_synced_in_workspace_properly():
    ws = MagicMock()

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    workspace = Group(display_name="data_engs", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup = Group(display_name="dbr_backup_data_engs", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    account = Group(display_name="data_engs", meta=ResourceMeta(resource_type="Group"))

    workspace_ds = Group(display_name="data_science", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup_ds = Group(display_name="dbr_backup_data_science", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    account_ds = Group(display_name="data_science", meta=ResourceMeta(resource_type="Group"))

    remote_state = GroupMigrationState()
    remote_state.add(workspace, backup, account)
    remote_state.add(workspace_ds, backup_ds, account_ds)

    ws.groups.list.return_value = [backup_ds, account_ds]
    ws.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account, account_ds]],
    }

    manager = GroupManager(ws, group_conf)
    new_state = manager.prepare_apply_permissions_to_account_groups(ws, remote_state, group_conf.backup_group_prefix)

    assert new_state.groups[0].workspace.display_name == "data_science"


def test_group_not_present_in_remote_state():
    ws = MagicMock()

    group_conf = GroupsConfig(backup_group_prefix="dbr_backup_", auto=True)
    backup = Group(display_name="dbr_backup_data_engs", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    account = Group(display_name="data_engs", meta=ResourceMeta(resource_type="Group"))

    workspace_ds = Group(display_name="data_science", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    backup_ds = Group(display_name="dbr_backup_data_science", meta=ResourceMeta(resource_type="WorkspaceGroup"))
    account_ds = Group(display_name="data_science", meta=ResourceMeta(resource_type="Group"))

    remote_state = GroupMigrationState()
    remote_state.add(workspace_ds, backup_ds, account_ds)

    ws.groups.list.return_value = [backup, account, backup_ds, account_ds]
    ws.api_client.do.return_value = {
        "Resources": [g.as_dict() for g in [account, account_ds]],
    }

    manager = GroupManager(ws, group_conf)
    new_state = manager.prepare_apply_permissions_to_account_groups(ws, remote_state, group_conf.backup_group_prefix)

    assert len(new_state) == 1
