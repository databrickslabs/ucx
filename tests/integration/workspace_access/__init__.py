from databricks.labs.ucx.framework.parallel import ManyError, Threads
from databricks.labs.ucx.workspace_access.base import AclSupport
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState


def apply_tasks(support: AclSupport, groups: list[MigratedGroup]):
    migration_state = MigrationState(groups)
    permissions, errs = Threads.gather("apply_tasks: crawlers", support.get_crawler_tasks())  # type: ignore[var-annotated,arg-type]
    if errs:
        raise ManyError(errs)
    apply_tasks = [support.get_apply_task(_, migration_state) for _ in permissions]
    _, errs = Threads.gather("apply_tasks: appliers", apply_tasks)  # type: ignore[arg-type]
    if errs:
        raise ManyError(errs)
