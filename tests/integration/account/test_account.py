from datetime import timedelta

import pytest
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import AccountClient
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import Group, User

from databricks.labs.ucx.account.workspaces import AccountWorkspaces


@pytest.mark.skip(reason="Test tests interferes with other tests")
def test_clean_test_users_in_account(acc: AccountClient) -> None:
    """Run this test to clean up the account from test users.

    Watchdog does not clean up the account from test users as it only looks
    at the workspace level. This test is not really a test, but a test
    utility to clean up the account from test users.
    """
    for user in acc.users.list(attributes="id", filter='displayName sw "dummy-"'):  # "sw" is short for "starts with"
        if user.id:
            acc.users.delete(user.id)
    # No assert as the API is eventually consistent and this test is skipped
    # Verify manually instead


@pytest.fixture
def clean_account_level_groups(acc: AccountClient):
    """Clean test generated account level groups."""

    def delete_ucx_created_groups():
        for group in acc.groups.list():
            if group.display_name.startswith("created_by_ucx"):
                acc.groups.delete(group.id)

    delete_ucx_created_groups()
    yield
    delete_ucx_created_groups()


def test_create_account_level_groups(
    make_ucx_group,
    make_group,
    make_user,
    make_run_as,
    acc,
    ws,
    make_random,
    clean_account_level_groups,
    watchdog_purge_suffix,
):
    suffix = f"{make_random(4).lower()}-{watchdog_purge_suffix}"
    make_ucx_group(f"test_ucx_migrate_invalid-{suffix}", f"test_ucx_migrate_invalid-{suffix}")

    group_display_name = f"created_by_ucx_regular_group-{suffix}"
    make_group(display_name=group_display_name, members=[make_user().id, make_run_as()._service_principal.id])
    AccountWorkspaces(acc, [ws.get_workspace_id()]).create_account_level_groups(MockPrompts({}))

    @retried(on=[KeyError], timeout=timedelta(minutes=2))
    def get_group(display_name: str) -> Group:
        for grp in acc.groups.list():
            if grp.display_name == display_name:
                return grp
        raise KeyError(f"Group not found {display_name}")

    group = get_group(group_display_name)
    assert group
    assert len(group.members) == 2  # 2 members: user and service principal


def test_create_account_level_groups_nested_groups(
    make_group, make_user, acc, ws, make_random, clean_account_level_groups, watchdog_purge_suffix, runtime_ctx, caplog
):
    suffix = f"{make_random(4).lower()}-{watchdog_purge_suffix}"
    # Test groups:
    # 1. group a contains group_b and group_c.
    # 2. group b contains user1, user2 and group_d.
    # 3. group c contains group_d.
    # 4. group d contains user3 and user4.

    users = list[User]()
    for _ in range(4):
        users.append(make_user())

    ws_groups = list[Group]()
    ws_groups.append(
        make_group(display_name=f"created_by_ucx_regular_group_d-{suffix}", members=[users[2].id, users[3].id])
    )
    ws_groups.append(make_group(display_name=f"created_by_ucx_regular_group_c-{suffix}", members=ws_groups[0].id))
    ws_groups.append(
        make_group(
            display_name=f"created_by_ucx_regular_group_b-{suffix}", members=[users[0].id, users[1].id, ws_groups[0].id]
        )
    )
    ws_groups.append(
        make_group(display_name=f"created_by_ucx_regular_group_a-{suffix}", members=[ws_groups[1].id, ws_groups[2].id])
    )

    AccountWorkspaces(acc, [ws.get_workspace_id()]).create_account_level_groups(MockPrompts({}))

    @retried(on=[KeyError], timeout=timedelta(minutes=2))
    def get_group(display_name: str) -> Group:
        for grp in acc.groups.list():
            if grp.display_name == display_name:
                return grp
        raise KeyError(f"Group not found {display_name}")

    for ws_group in ws_groups:
        group_display_name = ws_group.display_name
        group = get_group(group_display_name)
        assert group
        assert len(group.members) == len(ws_group.members)

    runtime_ctx.group_manager.validate_group_membership()

    assert 'There are no groups with different membership between account and workspace.' in caplog.text
