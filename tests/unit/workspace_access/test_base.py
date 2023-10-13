from collections.abc import Callable, Iterator
from functools import partial

from databricks.sdk.service import iam

from databricks.labs.ucx.workspace_access.base import (
    AclSupport,
    Destination,
    Permissions,
)
from databricks.labs.ucx.workspace_access.groups import MigrationState


def test_applier():
    class SampleApplier(AclSupport):
        def __init__(self):
            self.called = False

        def get_crawler_tasks(self) -> Iterator[Callable[..., Permissions | None]]:
            return []

        def object_types(self) -> set[str]:
            return {"test"}

        @staticmethod
        def _is_item_relevant(item: Permissions, migration_state: MigrationState) -> bool:
            workspace_groups = [info.workspace.display_name for info in migration_state.groups]
            return item.object_id in workspace_groups

        def get_apply_task(self, item: Permissions, migration_state: MigrationState, _: Destination):
            if not self._is_item_relevant(item, migration_state):
                return None

            def test_task():
                self.called = True
                print("here!")

            return partial(test_task)

    applier = SampleApplier()
    positive_item = Permissions(object_id="test", object_type="test", raw="test")
    migration_state = MigrationState()
    migration_state.add(
        iam.Group(display_name="test", id="test"),
        iam.Group(display_name="db-temp-test", id="test-backup"),
        iam.Group(display_name="test", id="test-acc"),
    )

    task = applier.get_apply_task(positive_item, migration_state, "backup")
    task()
    assert applier.called

    negative_item = Permissions(object_id="not-here", object_type="test", raw="test")
    task = applier.get_apply_task(negative_item, migration_state, "backup")
    assert task is None
