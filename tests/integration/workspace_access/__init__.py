from databricks.labs.ucx.workspace_access.base import AclSupport
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState


def apply_tasks(support: AclSupport, groups: list[MigratedGroup]):
    migration_state = MigrationState(groups)
    for crawler_task in support.get_crawler_tasks():
        permission = crawler_task()
        if not permission:
            continue
        apply_task = support.get_apply_task(permission, migration_state)
        if not apply_task:
            continue
        apply_task()
