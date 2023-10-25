import dataclasses
import functools
import json
from collections.abc import Callable, Iterator
from functools import partial

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.hive_metastore import GrantsCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.workspace_access.base import (
    AclSupport,
    Destination,
    Permissions,
)
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState


class TableAclSupport(AclSupport):
    def __init__(self, grants_crawler: GrantsCrawler, sql_backend: SqlBackend):
        self._grants_crawler = grants_crawler
        self._sql_backend = sql_backend

    def get_crawler_tasks(self) -> Iterator[Callable[..., Permissions | None]]:
        def inner(grant: Grant) -> Permissions:
            object_type, object_key = grant.this_type_and_key()
            return Permissions(object_type=object_type, object_id=object_key, raw=json.dumps(dataclasses.asdict(grant)))

        for grant in self._grants_crawler.snapshot():
            yield functools.partial(inner, grant)

    def object_types(self) -> set[str]:
        return {"TABLE", "DATABASE", "VIEW", "CATALOG", "ANONYMOUS FUNCTION", "ANY FILE"}

    def get_apply_task(self, item: Permissions, migration_state: GroupMigrationState, destination: Destination):
        grant = Grant(**json.loads(item.raw))
        target_principal = migration_state.get_target_principal(grant.principal, destination)
        if target_principal is None:
            # this is a grant for user, service principal, or irrelevant group
            return None
        target_grant = dataclasses.replace(grant, principal=target_principal)
        sql = target_grant.hive_grant_sql()
        # this has to be executed on tacl cluster, otherwise - use SQLExecutionAPI backend & Warehouse
        return partial(self._sql_backend.execute, sql)

    # TODO: enable once Table ACL Grants concurrency issue is fixed: https://databricks.atlassian.net/browse/ES-908737
    def concurrency_support_for_apply_task(self):
        return False
