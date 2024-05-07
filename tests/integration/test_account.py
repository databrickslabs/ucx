from datetime import timedelta

from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import AccountClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import Group

from databricks.labs.ucx.account.workspaces import AccountWorkspaces


def test_create_account_level_groups(make_ucx_group, make_group, make_user, acc, ws, make_random):
    suffix = make_random()
    make_ucx_group(f"test_ucx_migrate_invalid_{suffix}", f"test_ucx_migrate_invalid_{suffix}")

    group_display_name = f"regular_group_{suffix}"
    make_group(display_name=group_display_name, members=[make_user().id])
    AccountWorkspaces(acc).create_account_level_groups(MockPrompts({}), [ws.get_workspace_id()])

    @retried(on=[KeyError], timeout=timedelta(minutes=2))
    def get_group(display_name: str) -> Group:
        for grp in acc.groups.list():
            if grp.display_name == display_name:
                return grp
        raise KeyError(f"Group not found {display_name}")

    group = get_group(group_display_name)
    try_delete_group(acc, group.id)  # Avoids flakiness for future runs
    assert group


def try_delete_group(acc: AccountClient, grp_id: str):
    try:
        acc.groups.delete(grp_id)
    except NotFound:
        pass
