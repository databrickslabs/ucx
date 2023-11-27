import json
import logging
from datetime import timedelta

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import PermissionLevel, ResourceMeta

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


def test_prepare_environment(ws, make_ucx_group, sql_backend, inventory_schema):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_migration_state = group_manager.snapshot()

    assert len(group_migration_state) == 1
    assert group_migration_state[0].id_in_workspace == ws_group.id
    assert group_migration_state[0].external_id == acc_group.id
    assert group_migration_state[0].name_in_workspace == ws_group.display_name
    assert group_migration_state[0].name_in_account == acc_group.display_name
    assert group_migration_state[0].temporary_name == "ucx-temp-" + ws_group.display_name
    assert len(group_migration_state[0].members) == len(json.dumps([gg.as_dict() for gg in ws_group.members]))
    assert not group_migration_state[0].roles
    assert len(group_migration_state[0].entitlements) == len(json.dumps([gg.as_dict() for gg in ws_group.entitlements]))


def test_prepare_environment_no_groups_selected(ws, make_ucx_group, sql_backend, inventory_schema):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema)
    group_migration_state = group_manager.snapshot()

    names = {info.name_in_workspace: info for info in group_migration_state}
    assert ws_group.display_name in names


def test_rename_groups(ws, make_ucx_group, sql_backend, inventory_schema):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_manager.rename_groups()

    assert ws.groups.get(ws_group.id).display_name == "ucx-temp-" + ws_group.display_name


def test_reflect_account_groups_on_workspace_throws_when_group_already_exists(
    ws, make_ucx_group, sql_backend, inventory_schema
):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    with pytest.raises(RuntimeWarning):
        group_manager.reflect_account_groups_on_workspace()


def test_reflect_account_groups_on_workspace(ws, make_ucx_group, sql_backend, inventory_schema):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_manager.rename_groups()
    group_manager.reflect_account_groups_on_workspace()

    reflected_group = ws.groups.get(acc_group.id)
    assert reflected_group.display_name == ws_group.display_name == acc_group.display_name
    assert {info.display for info in reflected_group.members} == {info.display for info in ws_group.members}
    assert {info.display for info in reflected_group.members} == {info.display for info in acc_group.members}
    assert reflected_group.meta == ResourceMeta(resource_type="Group")
    assert not reflected_group.roles  # Cannot create roles currently
    assert not reflected_group.entitlements  # Entitlements aren't reflected there

    assert (
        ws.groups.get(ws_group.id).display_name == "ucx-temp-" + ws_group.display_name
    )  # At this time previous ws level groups aren't deleted


def test_delete_ws_groups_should_delete_renamed_and_reflected_groups_only(
    ws, make_ucx_group, sql_backend, inventory_schema
):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_manager.rename_groups()
    group_manager.reflect_account_groups_on_workspace()
    group_manager.delete_original_workspace_groups()

    with pytest.raises(NotFound):
        ws.groups.get(ws_group.id)


def test_delete_ws_groups_should_not_delete_current_ws_groups(ws, make_ucx_group, sql_backend, inventory_schema):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_manager.delete_original_workspace_groups()

    assert ws.groups.get(ws_group.id).display_name == ws_group.display_name


def test_delete_ws_groups_should_not_delete_non_reflected_acc_groups(ws, make_ucx_group, sql_backend, inventory_schema):
    ws_group, acc_group = make_ucx_group()
    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_manager.rename_groups()
    group_manager.delete_original_workspace_groups()

    assert ws.groups.get(ws_group.id).display_name == "ucx-temp-" + ws_group.display_name


@retried(on=[NotFound, TimeoutError, AssertionError], timeout=timedelta(minutes=20))
def test_replace_workspace_groups_with_account_groups(
    ws,
    sql_backend,
    inventory_schema,
    make_ucx_group,
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

    tables = TablesCrawler(sql_backend, inventory_schema)
    grants = GrantsCrawler(tables)

    dummy_table = make_table()
    sql_backend.execute(f"GRANT SELECT, MODIFY ON TABLE {dummy_table.full_name} TO `{ws_group.display_name}`")
    res = grants.for_table_info(dummy_table)
    assert len(res[ws_group.display_name]) == 2

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")

    generic_permissions = GenericPermissionsSupport(
        ws, [Listing(ws.cluster_policies.list, "policy_id", "cluster-policies")]
    )

    tacl = TableAclSupport(grants, sql_backend)
    permission_manager = PermissionManager(sql_backend, inventory_schema, [generic_permissions, tacl])

    permission_manager.inventorize_permissions()

    dummy_grants = list(permission_manager.load_all_for("TABLE", dummy_table.full_name, Grant))
    assert 2 == len(dummy_grants)

    table_permissions = grants.for_table_info(dummy_table)
    assert ws_group.display_name in table_permissions
    assert "MODIFY" in table_permissions[ws_group.display_name]
    assert "SELECT" in table_permissions[ws_group.display_name]

    state = group_manager.get_migration_state()
    assert len(state) == 1

    group_manager.rename_groups()

    group_info = state.groups[0]

    @retried(on=[AssertionError], timeout=timedelta(seconds=30))
    def check_permissions_for_backup_group():
        logger.info("check_permissions_for_backup_group()")

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.name_in_workspace not in table_permissions
        assert group_info.temporary_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.temporary_name]
        assert "SELECT" in table_permissions[group_info.temporary_name]

        policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
        assert PermissionLevel.CAN_USE == policy_permissions[group_info.temporary_name]

    check_permissions_for_backup_group()

    group_manager.reflect_account_groups_on_workspace()

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_permissions_after_replace():
        logger.info("check_permissions_after_replace()")

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.name_in_account not in table_permissions
        assert group_info.temporary_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.temporary_name]
        assert "SELECT" in table_permissions[group_info.temporary_name]

        policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
        assert group_info.name_in_workspace not in policy_permissions
        assert PermissionLevel.CAN_USE == policy_permissions[group_info.temporary_name]

    check_permissions_after_replace()

    permission_manager.apply_group_permissions(state)

    @retried(on=[AssertionError], timeout=timedelta(seconds=30))
    def check_permissions_for_account_group():
        logger.info("check_permissions_for_account_group()")

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.name_in_account in table_permissions
        assert group_info.temporary_name in table_permissions
        assert "MODIFY" in table_permissions[group_info.temporary_name]
        assert "SELECT" in table_permissions[group_info.temporary_name]
        assert "MODIFY" in table_permissions[group_info.name_in_account]
        assert "SELECT" in table_permissions[group_info.name_in_account]

        policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
        assert PermissionLevel.CAN_USE == policy_permissions[group_info.name_in_account]
        assert PermissionLevel.CAN_USE == policy_permissions[group_info.temporary_name]

    check_permissions_for_account_group()

    group_manager.delete_original_workspace_groups()

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_table_permissions_after_backup_delete():
        logger.info("check_table_permissions_after_backup_delete()")

        policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
        assert group_info.temporary_name not in policy_permissions

        table_permissions = grants.for_table_info(dummy_table)
        assert group_info.name_in_account in table_permissions
        assert "MODIFY" in table_permissions[group_info.name_in_account]
        assert "SELECT" in table_permissions[group_info.name_in_account]

    check_table_permissions_after_backup_delete()
