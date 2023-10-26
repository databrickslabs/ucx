import collections
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
        # TableAcl grant/revoke operations are not atomic. When granting the permissions,
        # the service would first get all existing permissions, append with the new permissions,
        # and set the full list in the database. If there are concurrent grant requests,
        # both requests might succeed and emit the audit logs, but what actually happens could be that
        # the new permission list from one request overrides the other one, causing permissions loss.
        # More info here: https://databricks.atlassian.net/browse/ES-908737
        #
        # Below optimization mitigates the issue by folding all action types (grants)
        # for the same principal, object_id and object_type into one grant with comma separated list of action types.
        #
        # For example, the following table grants:
        # * GRANT SELECT ON TABLE hive_metastore.db_a.table_a TO group_a
        # * GRANT MODIFY ON TABLE hive_metastore.db_a.table_a TO group_a
        # will be folded and executed in one statement/transaction:
        # * GRANT SELECT, MODIFY ON TABLE hive_metastore.db_a.table_a TO group_a
        folded = collections.defaultdict(lambda: {"action_type": set()})
        for grant in self._grants_crawler.snapshot():
            key = (grant.principal, grant.this_type_and_key())
            folded[key]["action_type"].add(grant.action_type)
            grant_dict = dataclasses.asdict(grant)
            grant_dict["action_type"] = ", ".join(sorted(folded[key]["action_type"]))
            folded[key]["grant_folded"] = grant_dict

        def inner(obj_type: str, obj_key: str, grant_folded: dict) -> Permissions:
            return Permissions(object_type=obj_type, object_id=obj_key, raw=json.dumps(grant_folded))

        for (_principal, (object_type, object_key)), grant_data in folded.items():
            yield functools.partial(inner, object_type, object_key, grant_data["grant_folded"])

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
