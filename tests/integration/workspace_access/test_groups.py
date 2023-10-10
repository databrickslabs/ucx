from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.config import GroupsConfig
from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    listing_wrapper,
)
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager


def test_prepare_environment(ws, make_ucx_group):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    group_manager.prepare_groups_in_environment()

    group_migration_state = group_manager.migration_groups_provider
    for _info in group_migration_state.groups:
        _ws = ws.groups.get(id=_info.workspace.id)
        _backup = ws.groups.get(id=_info.backup.id)
        _ws_members = sorted([m.value for m in _ws.members])
        _backup_members = sorted([m.value for m in _backup.members])
        assert _ws_members == _backup_members


def test_prepare_environment_no_groups_selected(ws, make_ucx_group, make_group, make_acc_group):
    make_group()
    make_acc_group()
    for_test = [make_ucx_group(), make_ucx_group()]

    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    group_migration_state = group_manager.migration_groups_provider
    for _info in group_migration_state.groups:
        _ws = ws.groups.get(id=_info.workspace.id)
        _backup = ws.groups.get(id=_info.backup.id)
        # https://github.com/databricks/databricks-sdk-py/pull/361 may fix the NPE gotcha with empty members
        _ws_members = sorted([m.value for m in _ws.members]) if _ws.members is not None else []
        _backup_members = sorted([m.value for m in _backup.members]) if _backup.members is not None else []
        assert _ws_members == _backup_members

    for g, _ in for_test:
        assert group_migration_state.get_by_workspace_group_name(g.display_name) is not None


def test_group_listing(ws: WorkspaceClient, make_ucx_group):
    ws_group, acc_group = make_ucx_group()
    manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    assert ws_group.display_name in [g.display_name for g in manager._workspace_groups]
    assert acc_group.display_name in [g.display_name for g in manager._account_groups]


def test_id_validity(ws: WorkspaceClient, make_ucx_group):
    ws_group, acc_group = make_ucx_group()
    manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    assert ws_group.id == manager._get_group(ws_group.display_name, "workspace").id
    assert acc_group.id == manager._get_group(acc_group.display_name, "account").id


def test_recover_from_ws_local_deletion(ws, make_ucx_group):
    ws_group, _ = make_ucx_group()
    ws_group_two, _ = make_ucx_group()

    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    # simulate disaster
    ws.groups.delete(ws_group.id)
    ws.groups.delete(ws_group_two.id)

    # recovery run from a debug notebook
    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.ws_local_group_deletion_recovery()

    # normal run after from a job
    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    migration_state = group_manager.migration_groups_provider

    recovered_state = {}
    for gi in migration_state.groups:
        recovered_state[gi.workspace.display_name] = gi.workspace

    assert sorted([member.display for member in ws_group.members]) == sorted(
        [member.display for member in recovered_state[ws_group.display_name].members]
    )
    assert sorted([member.display for member in ws_group_two.members]) == sorted(
        [member.display for member in recovered_state[ws_group_two.display_name].members]
    )

    assert sorted([member.value for member in ws_group.members]) == sorted(
        [member.value for member in recovered_state[ws_group.display_name].members]
    )

    assert sorted([member.value for member in ws_group_two.members]) == sorted(
        [member.value for member in recovered_state[ws_group_two.display_name].members]
    )


def test_recover_after_backup_group_creation(
    ws,
    sql_backend,
    inventory_schema,
    make_ucx_group,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_table,
    make_directory,
):
    """Testing the following scenario:
    1. backup groups created
    2. workspace-local groups deleted
    3. corresponding account group attached
    4. no permissions applied
    5. restarting the tool to re-apply permissions."""

    #
    # setup and inventorize assets
    #

    ws_group, acc_group = make_ucx_group()

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws, [listing_wrapper(ws.cluster_policies.list, "policy_id", "cluster-policies")]
    )

    schema_name = inventory_schema
    permission_manager = PermissionManager(
        sql_backend, schema_name, [generic_permissions], {"cluster-policies": generic_permissions}
    )

    permission_manager.inventorize_permissions()

    #
    # Simulate first run - until backup group creation
    #

    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()
    group_manager.replace_workspace_groups_with_account_groups()
    # not applying permissions to simulate it did not work

    #
    # Simulate restarting the tool to reapply permissions
    #

    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    directory = make_directory()
    permission_manager = PermissionManager.factory(
        ws,
        sql_backend,
        schema_name,
        workspace_start_path=directory,
    )

    permission_manager.apply_group_permissions(group_manager.migration_groups_provider, destination="backup")
    group_manager.replace_workspace_groups_with_account_groups()
    permission_manager.apply_group_permissions(group_manager.migration_groups_provider, destination="account")

    #
    # verify migration state and permissions
    #

    group_info = group_manager.migration_groups_provider.groups[0]
    assert group_info.workspace.display_name == ws_group.display_name
    assert not group_info.workspace.id
    assert group_info.backup.display_name == "db-temp-" + ws_group.display_name
    assert group_info.account.id == acc_group.id

    policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert PermissionLevel.CAN_USE == policy_permissions[group_info.backup.display_name]
    assert PermissionLevel.CAN_USE == policy_permissions[group_info.account.display_name]
