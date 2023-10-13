import pytest
from databricks.sdk.service import iam

from databricks.labs.ucx.workspace_access.groups import MigrationState


@pytest.fixture(scope="function")
def migration_state() -> MigrationState:
    ms = MigrationState()
    ms.add(
        iam.Group(display_name="test", id="test-ws"),
        iam.Group(display_name="db-temp-test", id="test-backup"),
        iam.Group(display_name="test", id="test-acc"),
    )
    return ms
