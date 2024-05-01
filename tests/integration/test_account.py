from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import AccountClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.account.workspaces import AccountWorkspaces


def test_create_account_level_groups(make_ucx_group, make_group, make_user, acc, ws, make_random):
    suffix = make_random()
    make_ucx_group(f"test_ucx_migrate_invalid_{suffix}", f"test_ucx_migrate_invalid_{suffix}")

    make_group(display_name=f"regular_group_{suffix}", members=[make_user().id])
    AccountWorkspaces(acc).create_account_level_groups(MockPrompts({}), [ws.get_workspace_id()])

    results = []
    for grp in acc.groups.list():
        if grp.display_name in {f"regular_group_{suffix}"}:
            results.append(grp)
            try_delete_group(acc, grp.id)  # Avoids flakiness for future runs

    assert len(results) == 1


def try_delete_group(acc: AccountClient, grp_id: str):
    try:
        acc.groups.delete(grp_id)
    except NotFound:
        pass
