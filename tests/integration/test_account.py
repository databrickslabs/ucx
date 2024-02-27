from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.account import AccountWorkspaces


def test_create_account_level_groups(make_ucx_group, make_group, make_user, acc, ws):
    make_ucx_group("test_ucx_migrate_invalid", "test_ucx_migrate_invalid")

    make_group(display_name="regular_group", members=[make_user().id])
    AccountWorkspaces(acc).create_account_level_groups(MockPrompts({}), [ws.get_workspace_id()])

    results = []
    for grp in acc.groups.list():
        if grp.display_name in ["regular_group"]:
            results.append(grp)
            acc.groups.delete(grp.id)  # Avoids flakiness for future runs

    assert len(results) == 1
