from collections.abc import Callable, Iterable

from databricks.labs.ucx.workspace_access.base import Permissions, AclSupport
from databricks.labs.ucx.workspace_access.groups import MigrationState
from databricks.labs.ucx.workspace_access.manager import PermissionManager


def test_permissions_snapshot(ws, sql_backend, inventory_schema):
    class StubbedCrawler(AclSupport):
        def get_crawler_tasks(self) -> Iterable[Callable[..., Permissions | None]]:
            yield lambda: Permissions(object_id="abc", object_type="bcd", raw="def")
            yield lambda: Permissions(object_id="efg", object_type="fgh", raw="ghi")

        def get_apply_task(self, item: Permissions, migration_state: MigrationState) -> Callable[[], None] | None: ...
        def get_verify_task(self, item: Permissions) -> Callable[[], bool] | None: ...
        def object_types(self) -> set[str]:
            return {"bcd", "fgh"}

    permission_manager = PermissionManager(sql_backend, inventory_schema, [StubbedCrawler()])
    snapshot = list(permission_manager.snapshot())
    # Snapshotting is multithreaded, meaning the order of results is non-deterministic.
    snapshot.sort(key=lambda x: x.object_id)

    expected = [
        Permissions(object_id="abc", object_type="bcd", raw="def"),
        Permissions(object_id="efg", object_type="fgh", raw="ghi"),
    ]
    assert snapshot == expected

    saved = [
        Permissions(*row)
        for row in sql_backend.fetch(
            f"SELECT object_id, object_type, raw\n"
            f"FROM {inventory_schema}.permissions\n"
            f"ORDER BY object_id\n"
            f"LIMIT {len(expected)+1}"
        )
    ]
    assert saved == expected
