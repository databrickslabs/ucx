from databricks.labs.blueprint.parallel import ManyError, Threads

from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState


def apply_tasks(support: AclSupport, groups: list[MigratedGroup]):
    permissions = apply_tasks_crawlers(support)
    migration_state = MigrationState(groups)
    apply_tasks_appliers(support, permissions, migration_state)


def apply_tasks_crawlers(support: AclSupport) -> list[Permissions]:
    permissions, errs = Threads.gather("apply_tasks: crawlers", support.get_crawler_tasks())  # type: ignore[var-annotated,arg-type]
    if errs:
        raise ManyError(errs)
    return permissions


def apply_tasks_appliers(support: AclSupport, permissions: list[Permissions], migration_state: MigrationState):
    tasks = [support.get_apply_task(_, migration_state) for _ in permissions]
    _, errs = Threads.gather("apply_tasks: appliers", tasks)  # type: ignore[arg-type]
    if errs:
        raise ManyError(errs)
