from collections.abc import Callable, Iterable
from functools import partial

from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState


def test_applier():
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
                self.verify(item.object_type, item.object_id, [])
                print("here!")

            return partial(test_task)

        def verify(self, object_type: str, object_id: str, acl: list) -> bool:
            return True

    applier = SampleApplier()
    positive_item = Permissions(object_id="test", object_type="test", raw="test")
    migration_state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace=None,
                name_in_workspace="test",
                name_in_account="test",
                temporary_name="db-temp-test",
                members=None,
                entitlements=None,
                external_id=None,
                roles=None,
            )
        ]
    )

    task = applier.get_apply_task(positive_item, migration_state)
    task()
    assert applier.called

    negative_item = Permissions(object_id="not-here", object_type="test", raw="test")
    task = applier.get_apply_task(negative_item, migration_state)
    assert task is None
