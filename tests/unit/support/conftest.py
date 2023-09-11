import pytest
from databricks.sdk.service import iam

from databricks.labs.ucx.providers.groups_info import (
    GroupMigrationState,
    MigrationGroupInfo,
)


@pytest.fixture(scope="function")
def migration_state() -> GroupMigrationState:
    ms = GroupMigrationState()
    ms.add(
        group=MigrationGroupInfo(
            workspace=iam.Group(display_name="test", id="test-ws"),
            backup=iam.Group(display_name="db-temp-test", id="test-backup"),
            account=iam.Group(display_name="test", id="test-acc"),
        )
    )
    return ms
