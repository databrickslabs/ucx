import logging
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import (
    ComplexValue,
    Group,
    PermissionLevel,
    Patch,
    ResourceMeta,
)
from databricks.sdk.service import iam

from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Listing,
)
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

logger = logging.getLogger(__name__)


def test_renaming_groups(
    ws,
    sql_backend,
    inventory_schema,
    make_ucx_group,
    make_group,
    make_acc_group,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_table,
):
    ws_group, acc_group = make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )

    dummy_table = make_table()
    sql_backend.execute(f"GRANT SELECT ON TABLE {dummy_table.full_name} TO `{ws_group.display_name}`")

    ws.groups.patch(
        ws_group.id,
        operations=[iam.Patch(op=iam.PatchOp.REPLACE, path="displayName", value=f"UPDATED_{ws_group.display_name}")],
    )

    # group_manager = GroupManager(ws, GroupsConfig(auto=True))
    # group_manager.prepare_groups_in_environment()

    # group_info = group_manager.migration_state.get_by_workspace_group_name(ws_group.display_name)

    generic_permissions = GenericPermissionsSupport(ws, [])

    generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)

    tables = TablesCrawler(sql_backend, inventory_schema)
    grants = GrantsCrawler(tables)
    grants.for_table_info(dummy_table)

    tacl = TableAclSupport(grants, sql_backend)

    PermissionManager(sql_backend, inventory_schema, [generic_permissions, tacl])


def test_prepare_environment(ws, make_ucx_group):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(ws, GroupsConfig(selected=[ws_group.display_name]))
    group_manager.prepare_groups_in_environment()

    group_migration_state = group_manager.migration_state
    for _info in group_migration_state.groups:
        try:
            _ws = ws.groups.get(id=_info.workspace.id)
            _backup = ws.groups.get(id=_info.backup.id)
            _ws_members = sorted([m.value for m in _ws.members])
            _backup_members = sorted([m.value for m in _backup.members])
            assert _ws_members == _backup_members
        except NotFound:
            continue

    for _info in group_migration_state.groups:
        try:
            # cleanup side-effect
            ws.groups.delete(_info.backup.id)
        except NotFound:
            continue


def test_prepare_environment_no_groups_selected(ws, make_ucx_group, make_group, make_acc_group):
    make_group()
    make_acc_group()
    for_test = [make_ucx_group(), make_ucx_group()]

    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    group_migration_state = group_manager.migration_state
    for _info in group_migration_state.groups:
        _ws = ws.groups.get(id=_info.workspace.id)
        _backup = ws.groups.get(id=_info.backup.id)
        # https://github.com/databricks/databricks-sdk-py/pull/361 may fix the NPE gotcha with empty members
        _ws_members = sorted([m.value for m in _ws.members]) if _ws.members is not None else []
        _backup_members = sorted([m.value for m in _backup.members]) if _backup.members is not None else []
        assert _ws_members == _backup_members

    for g, _ in for_test:
        assert group_migration_state.get_by_workspace_group_name(g.display_name) is not None

    for _info in group_migration_state.groups:
        # cleanup side-effect
        ws.groups.delete(_info.backup.id)


def test_replace_workspace_groups_with_account_groups(
    ws,
    sql_backend,
    inventory_schema,
    make_ucx_group,
    make_group,
    make_acc_group,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_table,
):
    ws_group, _ = make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )
    logger.info(f"Cluster policy: {ws.config.host}#setting/clusters/cluster-policies/view/{cluster_policy.policy_id}")

    dummy_table = make_table()
    sql_backend.execute(f"GRANT SELECT, MODIFY ON TABLE {dummy_table.full_name} TO `{ws_group.display_name}`")

    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    group_info = group_manager.migration_state.get_by_workspace_group_name(ws_group.display_name)

    generic_permissions = GenericPermissionsSupport(
        ws, [Listing(ws.cluster_policies.list, "policy_id", "cluster-policies")]
    )
    tables = TablesCrawler(sql_backend, inventory_schema)
    grants = GrantsCrawler(tables)
    tacl = TableAclSupport(grants, sql_backend)
    permission_manager = PermissionManager(sql_backend, inventory_schema, [generic_permissions, tacl])

    permission_manager.inventorize_permissions()

    dummy_grants = list(permission_manager.load_all_for("TABLE", dummy_table.full_name, Grant))
    assert 2 == len(dummy_grants)

    table_permissions = grants.for_table_info(dummy_table)
    assert ws_group.display_name in table_permissions
    assert "MODIFY" in table_permissions[ws_group.display_name]
    assert "SELECT" in table_permissions[ws_group.display_name]

    permission_manager.apply_group_permissions(group_manager.migration_state, destination="backup")

    @retried(on=[AssertionError], timeout=timedelta(seconds=30))
    def check_permissions_for_backup_group():
        logger.info("check_permissions_for_backup_group()")

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.workspace.display_name in table_permissions
        assert group_info.backup.display_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.workspace.display_name]
        assert "SELECT" in table_permissions[group_info.workspace.display_name]
        assert "MODIFY" in table_permissions[group_info.backup.display_name]
        assert "SELECT" in table_permissions[group_info.backup.display_name]

        policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
        assert PermissionLevel.CAN_USE == policy_permissions[group_info.workspace.display_name]
        assert PermissionLevel.CAN_USE == policy_permissions[group_info.backup.display_name]

    check_permissions_for_backup_group()

    group_manager.replace_workspace_groups_with_account_groups(group_manager.migration_state)

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_permissions_after_replace():
        logger.info("check_permissions_after_replace()")

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.account.display_name in table_permissions
        assert group_info.backup.display_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.backup.display_name]
        assert "SELECT" in table_permissions[group_info.backup.display_name]

        policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
        assert group_info.workspace.display_name not in policy_permissions
        assert PermissionLevel.CAN_USE == policy_permissions[group_info.backup.display_name]

    check_permissions_after_replace()

    permission_manager.apply_group_permissions(group_manager.migration_state, destination="account")

    @retried(on=[AssertionError], timeout=timedelta(seconds=30))
    def check_permissions_for_account_group():
        logger.info("check_permissions_for_account_group()")

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.account.display_name in table_permissions
        assert group_info.backup.display_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.backup.display_name]
        assert "SELECT" in table_permissions[group_info.backup.display_name]
        assert "MODIFY" in table_permissions[group_info.account.display_name]
        assert "SELECT" in table_permissions[group_info.account.display_name]

        policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
        assert PermissionLevel.CAN_USE == policy_permissions[group_info.account.display_name]
        assert PermissionLevel.CAN_USE == policy_permissions[group_info.backup.display_name]

    check_permissions_for_account_group()

    for _info in group_manager.migration_state.groups:
        ws.groups.delete(_info.backup.id)

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_table_permissions_after_backup_delete():
        logger.info("check_table_permissions_after_backup_delete()")

        policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
        assert group_info.backup.display_name not in policy_permissions

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.account.display_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.account.display_name]
        assert "SELECT" in table_permissions[group_info.account.display_name]

    check_table_permissions_after_backup_delete()


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
    ws_group, acc_group = make_ucx_group()
    ws_group_two, _ = make_ucx_group()

    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    # simulate disaster
    ws.groups.delete(ws_group.id)
    ws.groups.delete(ws_group_two.id)
    group_manager._reflect_account_group_to_workspace(acc_group)

    # recovery run from a debug notebook
    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.ws_local_group_deletion_recovery()

    # normal run after from a job
    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    recovered_state = {}
    for gi in group_manager.migration_state.groups:
        recovered_state[gi.workspace.display_name] = gi.workspace

    assert sorted([member.display for member in ws_group_two.members]) == sorted(
        [member.display for member in recovered_state[ws_group_two.display_name].members]
    )

    assert sorted([member.value for member in ws_group_two.members]) == sorted(
        [member.value for member in recovered_state[ws_group_two.display_name].members]
    )


def test_migration_state_should_be_saved_with_proper_values(sql_backend, make_schema):
    inventory_database = make_schema()

    workspace = Group(
        display_name="workspace", entitlements=[ComplexValue(display="entitlements", value="allow-cluster-create")]
    )
    backup = Group(display_name="db-temp-workspace", meta=ResourceMeta("test"))
    account = Group(display_name="account")

    state = GroupMigrationState()
    state.add(workspace, backup, account)

    state.persist_migration_state(sql_backend, inventory_database.name)
    new_state = state.fetch_migration_state(sql_backend, inventory_database.name)

    assert new_state.groups == state.groups


def test_migration_state_should_be_saved_without_missing_anything(sql_backend, make_schema):
    inventory_database = make_schema()

    workspace = Group("workspace")
    backup = Group("db-temp-workspace")
    account = Group("workspace")

    state = GroupMigrationState()
    state.add(workspace, backup, account)
    state.add(workspace, backup, None)
    state.add(workspace, None, account)
    state.add(None, None, account)
    state.add(None, None, None)

    state.persist_migration_state(sql_backend, inventory_database.name)
    new_state = state.fetch_migration_state(sql_backend, inventory_database.name)

    assert len(new_state.groups) == len(state.groups)


def test_set_owner_permission(
    ws,
    sql_backend,
    inventory_schema,
    make_ucx_group,
    make_group,
    make_acc_group,
    make_cluster_policy,
    make_table,
):
    ws_group, _ = make_ucx_group()

    logger.info("Testing setting ownership on table.")
    dummy_table = make_table()
    logger.info(f"Table name {dummy_table.full_name} group name {ws_group.display_name}")
    sql_backend.execute(f"GRANT SELECT, MODIFY ON TABLE {dummy_table.full_name} TO `{ws_group.display_name}`")
    sql_backend.execute(f"ALTER {dummy_table.full_name} OWNER TO `{ws_group.display_name}`")

    group_manager = GroupManager(ws, GroupsConfig(auto=True))
    group_manager.prepare_groups_in_environment()

    group_info = group_manager.migration_state.get_by_workspace_group_name(ws_group.display_name)

    tables = TablesCrawler(sql_backend, inventory_schema)
    grants = GrantsCrawler(tables)
    tacl = TableAclSupport(grants, sql_backend)
    permission_manager = PermissionManager(sql_backend, inventory_schema, [tacl])

    permission_manager.inventorize_permissions()

    dummy_grants = list(permission_manager.load_all_for("TABLE", dummy_table.full_name, Grant))
    assert 2 == len(dummy_grants)

    table_permissions = grants.for_table_info(dummy_table)
    assert ws_group.display_name in table_permissions
    assert "MODIFY" in table_permissions[ws_group.display_name]
    assert "SELECT" in table_permissions[ws_group.display_name]
    assert "OWN" in table_permissions[ws_group.display_name]

    permission_manager.apply_group_permissions(group_manager.migration_state, destination="backup")

    @retried(on=[AssertionError], timeout=timedelta(seconds=30))
    def check_permissions_for_backup_group():
        logger.info("check_permissions_for_backup_group()")

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.workspace.display_name in table_permissions
        assert group_info.backup.display_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.workspace.display_name]
        assert "SELECT" in table_permissions[group_info.workspace.display_name]
        assert "MODIFY" in table_permissions[group_info.backup.display_name]
        assert "SELECT" in table_permissions[group_info.backup.display_name]
        assert "OWN" in table_permissions[group_info.backup.display_name]

    check_permissions_for_backup_group()

    group_manager.replace_workspace_groups_with_account_groups(group_manager.migration_state)

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_permissions_after_replace():
        logger.info("check_permissions_after_replace()")

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.account.display_name in table_permissions
        assert group_info.backup.display_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.backup.display_name]
        assert "SELECT" in table_permissions[group_info.backup.display_name]
        assert "OWN" in table_permissions[group_info.backup.display_name]

    check_permissions_after_replace()

    permission_manager.apply_group_permissions(group_manager.migration_state, destination="account")

    @retried(on=[AssertionError], timeout=timedelta(seconds=30))
    def check_permissions_for_account_group():
        logger.info("check_permissions_for_account_group()")

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.account.display_name in table_permissions
        assert group_info.backup.display_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.backup.display_name]
        assert "SELECT" in table_permissions[group_info.backup.display_name]
        assert "MODIFY" in table_permissions[group_info.account.display_name]
        assert "SELECT" in table_permissions[group_info.account.display_name]
        assert "OWN" in table_permissions[group_info.account.display_name]

    check_permissions_for_account_group()

    for _info in group_manager.migration_state.groups:
        ws.groups.delete(_info.backup.id)

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_table_permissions_after_backup_delete():
        logger.info("check_table_permissions_after_backup_delete()")

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.account.display_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.account.display_name]
        assert "SELECT" in table_permissions[group_info.account.display_name]
        assert "OWN" in table_permissions[group_info.account.display_name]

    check_table_permissions_after_backup_delete()
