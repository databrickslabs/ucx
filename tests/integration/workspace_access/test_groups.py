import json
import logging
from datetime import timedelta

import pytest
from databricks.sdk.errors import NotFound, ResourceConflict
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import Group, ResourceMeta

from databricks.labs.ucx.workspace_access.groups import GroupManager


logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_prepare_environment(ws, make_ucx_group, sql_backend, inventory_schema):
    ws_group, acc_group = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_migration_state = group_manager.snapshot()

    assert len(group_migration_state) == 1
    assert group_migration_state[0].id_in_workspace == ws_group.id
    assert group_migration_state[0].name_in_workspace == ws_group.display_name
    assert group_migration_state[0].name_in_account == acc_group.display_name
    assert group_migration_state[0].temporary_name == "ucx-temp-" + ws_group.display_name
    assert len(group_migration_state[0].members) == len(json.dumps([gg.as_dict() for gg in ws_group.members]))
    assert not group_migration_state[0].roles
    assert len(group_migration_state[0].entitlements) == len(json.dumps([gg.as_dict() for gg in ws_group.entitlements]))


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_prepare_environment_no_groups_selected(ws, make_ucx_group, sql_backend, inventory_schema):
    ws_group, _ = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema)
    group_migration_state = group_manager.snapshot()

    names = {info.name_in_workspace: info for info in group_migration_state}
    assert ws_group.display_name in names


# group rename is eventually consistent
@retried(on=[AssertionError], timeout=timedelta(minutes=1))
def check_group_renamed(ws, ws_group):
    assert ws.groups.get(ws_group.id).display_name == "ucx-temp-" + ws_group.display_name


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_rename_groups(ws, make_ucx_group, sql_backend, inventory_schema):
    # FIXME - test_rename_groups - TimeoutError: Timed out after 0:01:00
    ws_group, _ = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_manager.rename_groups()

    check_group_renamed(ws, ws_group)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_reflect_account_groups_on_workspace_recovers_when_group_already_exists(
    ws, make_ucx_group, sql_backend, inventory_schema
):
    ws_group, _ = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_manager.reflect_account_groups_on_workspace()


@retried(on=[NotFound], timeout=timedelta(minutes=2))
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

    check_group_renamed(ws, ws_group)
    # At this time previous ws level groups aren't deleted


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_delete_ws_groups_should_delete_renamed_and_reflected_groups_only(
    ws, make_ucx_group, sql_backend, inventory_schema
):
    ws_group, _ = make_ucx_group()

    group_manager = GroupManager(
        sql_backend,
        ws,
        inventory_schema,
        [ws_group.display_name],
        "ucx-temp-",
    )
    group_manager.rename_groups()
    group_manager.reflect_account_groups_on_workspace()
    group_manager.delete_original_workspace_groups()

    # The API needs a moment to delete a group, i.e. until the group is not found anymore
    @retried(on=[KeyError], timeout=timedelta(minutes=2))
    def get_group(group_id: str):
        ws.groups.get(group_id)
        raise KeyError(f"Group is not deleted: {group_id}")

    with pytest.raises(NotFound, match=f"Group with id {ws_group.id} not found."):
        get_group(ws_group.id)


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_delete_ws_groups_should_not_delete_current_ws_groups(ws, make_ucx_group, sql_backend, inventory_schema):
    ws_group, _ = make_ucx_group()

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_manager.delete_original_workspace_groups()

    assert ws.groups.get(ws_group.id).display_name == ws_group.display_name


@retried(on=[NotFound, ResourceConflict], timeout=timedelta(minutes=3))
def test_delete_ws_groups_should_not_delete_non_reflected_acc_groups(ws, make_ucx_group, sql_backend, inventory_schema):
    ws_group, _ = make_ucx_group()
    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    group_manager.rename_groups()
    group_manager.delete_original_workspace_groups()

    check_group_renamed(ws, ws_group)


def validate_migrate_groups(group_manager: GroupManager, ws_group: Group, to_group: Group):
    if not group_manager.has_workspace_group(ws_group.display_name):
        raise NotFound(f'missing workspace group: {ws_group.display_name}')
    group_manager.rename_groups()
    if not group_manager.has_workspace_group(f"ucx-temp-{ws_group.display_name}"):
        raise NotFound('missing temp group')
    group_manager.reflect_account_groups_on_workspace()
    if not group_manager.has_account_group(to_group.display_name):
        raise NotFound(f'missing account group: {to_group.display_name}')


@retried(on=[NotFound], timeout=timedelta(minutes=5))
@pytest.mark.parametrize("strategy", ["prefix", "suffix", "substitute", "matching"])
def test_group_name_change(ws, sql_backend, inventory_schema, make_ucx_group, make_random, strategy):
    random_element = f"ucx{make_random(4)}"
    ws_group, account_group = None, None
    workspace_group_regex, workspace_group_replace, account_group_regex = None, None, None
    match strategy:
        case "prefix":
            ws_group, account_group = make_ucx_group(random_element, f"SAMPLE_{random_element}")
            workspace_group_regex, workspace_group_replace = "^", "SAMPLE_"
        case "suffix":
            ws_group, account_group = make_ucx_group(random_element, f"{random_element}_SAMPLE")
            workspace_group_regex, workspace_group_replace = "$", "_SAMPLE"
        case "substitute":
            ws_group, account_group = make_ucx_group(f"ucx_engineering_{random_element}", f"ucx_eng_{random_element}")
            workspace_group_regex, workspace_group_replace = "engineering", "eng"
        case "matching":
            ws_group, account_group = make_ucx_group(f"test_group_{random_element}", f"same_group_[{random_element}]")
            workspace_group_regex, account_group_regex = r"([0-9a-zA-Z]*)$", r"\[([0-9a-zA-Z]*)\]"
    group_manager = GroupManager(
        sql_backend,
        ws,
        inventory_schema,
        [ws_group.display_name],
        "ucx-temp-",
        workspace_group_regex,
        workspace_group_replace,
        account_group_regex,
    )
    logger.info(
        f"Attempting Mapping From Workspace Group {ws_group.display_name} to "
        f"Account Group {account_group.display_name}"
    )
    validate_migrate_groups(group_manager, ws_group, account_group)


@retried(on=[NotFound], timeout=timedelta(minutes=2))
@pytest.mark.parametrize("same_user", [True, False])
def test_group_matching_names(
    ws, sql_backend, inventory_schema, make_random, make_user, make_group, make_acc_group, same_user
):
    rand_elem = make_random(4)
    workspace_group_name = f"test_group_{rand_elem}"
    account_group_name = f"same_group_[{rand_elem}]"
    user1 = make_user()
    members1 = [user1.id]
    members2 = [user1.id]
    if not same_user:
        user2 = make_user()
        members2 = [user2.id]
    ws_group = make_group(display_name=workspace_group_name, members=members1, entitlements=["allow-cluster-create"])
    acc_group = make_acc_group(display_name=account_group_name, members=members2)

    logger.info(
        f"Attempting Mapping From Workspace Group {ws_group.display_name} to Account Group {acc_group.display_name}"
    )
    group_manager = GroupManager(
        sql_backend,
        ws,
        inventory_schema,
        [ws_group.display_name],
        "ucx-temp-",
        r"([0-9a-zA-Z]*)$",
        None,
        r"\[([0-9a-zA-Z]*)\]",
    )

    membership = group_manager.validate_group_membership()
    if same_user:
        assert len(membership) == 0
    else:
        assert len(membership) > 0
