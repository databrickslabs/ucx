from datetime import timedelta

import pytest
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import AccountClient
from databricks.sdk.service.iam import Group

from databricks.labs.ucx.account.workspaces import AccountWorkspaces

from .retries import retried


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
    make_ucx_group, make_group, make_user, acc, ws, make_random, clean_account_level_groups
):
    suffix = make_random()
    make_ucx_group(f"test_ucx_migrate_invalid_{suffix}", f"test_ucx_migrate_invalid_{suffix}")

    group_display_name = f"created_by_ucx_regular_group_{suffix}"
    make_group(display_name=group_display_name, members=[make_user().id])
    AccountWorkspaces(acc).create_account_level_groups(MockPrompts({}), [ws.get_workspace_id()])

    @retried(on=[KeyError], timeout=timedelta(minutes=2))
    def get_group(display_name: str) -> Group:
        for grp in acc.groups.list():
            if grp.display_name == display_name:
                return grp
        raise KeyError(f"Group not found {display_name}")

    group = get_group(group_display_name)
    assert group
