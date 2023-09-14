from functools import partial

from databricks.sdk.service import iam

from databricks.labs.ucx.providers.groups_info import (
    GroupMigrationState,
    MigrationGroupInfo,
)
from databricks.labs.ucx.utils import noop
from databricks.labs.ucx.workspace_access.base import Applier
from databricks.labs.ucx.workspace_access.types import PermissionsInventoryItem


def test_applier():
    class SampleApplier(Applier):
        def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
            workspace_groups = [info.workspace.display_name for info in migration_state.groups]
            return item.object_id in workspace_groups

        def _get_apply_task(self, _, __, ___):
            def test_task():
                print("here!")

            return partial(test_task)

    applier = SampleApplier()
    positive_item = PermissionsInventoryItem(object_id="test", support="test", raw_object_permissions="test")
    migration_state = GroupMigrationState()
    migration_state.add(
        group=MigrationGroupInfo(
            workspace=iam.Group(display_name="test", id="test"),
            account=iam.Group(display_name="test", id="test-acc"),
            backup=iam.Group(display_name="db-temp-test", id="test-backup"),
        )
    )

    task = applier.get_apply_task(positive_item, migration_state, "backup")
    assert task.func.__name__ == "test_task"

    negative_item = PermissionsInventoryItem(object_id="not-here", support="test", raw_object_permissions="test")
    new_task = applier.get_apply_task(negative_item, migration_state, "backup")
    assert new_task.func == noop
