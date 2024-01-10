from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState


def test_state():
    groups = [
        MigratedGroup(
            id_in_workspace="1", name_in_workspace="test1", name_in_account="acc_test1", temporary_name="db-temp-test1"
        )
    ]

    state = MigrationState(groups)

    assert state.get_target_principal("test1") == "acc_test1"
    assert state.get_temp_principal("test1") == "db-temp-test1"
    assert state.is_in_scope("test1")

    assert not state.get_target_principal("invalid_group_name")
    assert not state.get_temp_principal("invalid_group_name")
    assert not state.is_in_scope("invalid_group_name")
