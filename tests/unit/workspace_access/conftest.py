import pytest

from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState


@pytest.fixture(scope="function")
def migration_state() -> MigrationState:
    grp = [
        MigratedGroup(
            id_in_workspace="test-ws",
            name_in_workspace="test",
            name_in_account="test",
            temporary_name="db-temp-test",
            members=None,
            entitlements=None,
            external_id=None,
            roles=None,
        )
    ]
    ms = MigrationState(grp)
    return ms
