import dataclasses
import functools
import json
from collections.abc import Callable, Iterator
from functools import partial

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.hive_metastore import GrantsCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.workspace_access.base import (
    Applier,
    Crawler,
    Destination,
    Permissions,
)
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState


class TableAclSupport(Crawler, Applier):
    def __init__(self, grants_crawler: GrantsCrawler, sql_backend: SqlBackend):
        self._grants_crawler = grants_crawler
        self._sql_backend = sql_backend

    def get_crawler_tasks(self) -> Iterator[Callable[..., Permissions | None]]:
        def inner(grant: Grant) -> Permissions:
            object_type, object_key = grant.type_and_key()
            return Permissions(object_type=object_type, object_id=object_key, raw=json.dumps(dataclasses.asdict(grant)))

        for grant in self._grants_crawler.snapshot():
            yield functools.partial(inner, grant)

    def is_item_relevant(self, _1: Permissions, _2: GroupMigrationState) -> bool:
        # TODO: this abstract method is a design flaw https://github.com/databrickslabs/ucx/issues/410
        return True

    def _noop(self):
        pass

    def _get_apply_task(
        self, item: Permissions, migration_state: GroupMigrationState, destination: Destination
    ) -> partial:
        grant = Grant(**json.loads(item.raw))
        target_principal = migration_state.get_target_principal(grant.principal, destination)
        if target_principal is None:
            # this is a grant for user, service principal, or irrelevant group
            # technically, we should be able just to `return self._noop`
            return partial(self._noop)
        target_grant = dataclasses.replace(grant, principal=target_principal)
        sql = target_grant.hive_grant_sql()
        # this has to be executed on tacl cluster, otherwise - use SQLExecutionAPI backend & Warehouse
        return partial(self._sql_backend.execute, sql)
