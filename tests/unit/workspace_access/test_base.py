from collections.abc import Callable, Iterable
from functools import partial

from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions
from databricks.labs.ucx.workspace_access.groups import MigrationState


def test_applier(migration_state):
    class SampleApplier(AclSupport):
        def __init__(self):
            self.called = False

        def get_crawler_tasks(self) -> Iterable[Callable[..., Permissions | None]]:
            return []

        def object_types(self) -> set[str]:
            return {"test"}

        @staticmethod
        def _is_item_relevant(item: Permissions, migration_state: MigrationState) -> bool:
            workspace_groups = [info.name_in_workspace for info in migration_state.groups]
            return item.object_id in workspace_groups

        def get_apply_task(self, item: Permissions, migration_state: MigrationState):
            if not self._is_item_relevant(item, migration_state):
                return None

            def test_task():
                self.called = True
                print("here!")

            return partial(test_task)

        def get_verify_task(self, item: Permissions):
            def test_task():
                self.called = True
                print("here!")

            return partial(test_task)

    applier = SampleApplier()
    positive_item = Permissions(object_id="test", object_type="test", raw="test")

    task = applier.get_apply_task(positive_item, migration_state)
    task()
    assert applier.called

    task = applier.get_verify_task(positive_item)
    task()
    assert applier.called

    negative_item = Permissions(object_id="not-here", object_type="test", raw="test")
    task = applier.get_apply_task(negative_item, migration_state)
    assert task is None
