from datetime import timedelta

import pytest
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import AccountClient
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import Group

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
    acc,
    ws,
    make_random,
    clean_account_level_groups,
    watchdog_purge_suffix,
):
    suffix = f"{make_random(4).lower()}-{watchdog_purge_suffix}"
    make_ucx_group(f"test_ucx_migrate_invalid-{suffix}", f"test_ucx_migrate_invalid-{suffix}")

    group_display_name = f"created_by_ucx_regular_group-{suffix}"
    make_group(display_name=group_display_name, members=[make_user().id])
    AccountWorkspaces(acc, [ws.get_workspace_id()]).create_account_level_groups(MockPrompts({}))

    @retried(on=[KeyError], timeout=timedelta(minutes=2))
    def get_group(display_name: str) -> Group:
        for grp in acc.groups.list():
            if grp.display_name == display_name:
                return grp
        raise KeyError(f"Group not found {display_name}")

    group = get_group(group_display_name)
    assert group


def test_create_account_level_groups_nested_groups(
    make_ucx_group,
    make_group,
    make_user,
    acc,
    ws,
    make_random,
    clean_account_level_groups,
    watchdog_purge_suffix,
):
    suffix = f"{make_random(4).lower()}-{watchdog_purge_suffix}"
    regular_group = make_group(display_name=f"created_by_ucx_regular_group-{suffix}", members=[make_user().id])

    group_display_name = f"created_by_ucx_nested_group-{suffix}"
    nested_group = make_group(display_name=group_display_name, members=[make_user().id, regular_group.id])
    AccountWorkspaces(acc, [ws.get_workspace_id()]).create_account_level_groups(MockPrompts({}))

    @retried(on=[KeyError], timeout=timedelta(minutes=2))
    def get_group(display_name: str) -> Group:
        for grp in acc.groups.list():
            if grp.display_name == display_name:
                return grp
        raise KeyError(f"Group not found {display_name}")

    group = get_group(group_display_name)
    assert group
    assert len(group.members) == len(nested_group.members)
