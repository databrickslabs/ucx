from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.account import AccountWorkspaces


def test_create_account_level_groups(make_ucx_group, make_group, make_user, acc):
    make_ucx_group("test_ucx_migrate_invalid", "test_ucx_migrate_invalid")

    members = []
    for i in range(10):
        user = make_user()
        members.append(user.id)

    make_group(display_name="test_ucx_migrate_valid", members=members, entitlements=["allow-cluster-create"])
    AccountWorkspaces(acc).create_account_level_groups(MockPrompts({}))

    results = []
    for grp in acc.groups.list():
        if grp.display_name == "test_ucx_migrate_valid":
            results.append(grp)

    assert len(results) == 1

