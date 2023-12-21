import collections
import dataclasses
import functools
import json
import threading
from collections.abc import Callable, Iterator
from functools import partial

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.hive_metastore import GrantsCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions
from databricks.labs.ucx.workspace_access.groups import MigrationState


class TableAclSupport(AclSupport):
    def __init__(self, grants_crawler: GrantsCrawler, sql_backend: SqlBackend):
        self._grants_crawler = grants_crawler
        self._sql_backend = sql_backend
        self._lock = threading.Lock()

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
        # The exception is OWN permission which are set with ALTER table

        folded_actions = collections.defaultdict(set)
        for grant in self._grants_crawler.snapshot():
            key = (grant.principal, grant.this_type_and_key())
            folded_actions[key].add(grant.action_type)

        def inner(object_type: str, object_id: str, grant: Grant) -> Permissions:
            return Permissions(object_type=object_type, object_id=object_id, raw=json.dumps(dataclasses.asdict(grant)))

        for (principal, (object_type, object_id)), actions in folded_actions.items():
            grant = self._from_reduced(object_type, object_id, principal, ", ".join(sorted(actions)))
            yield functools.partial(inner, object_type=object_type, object_id=object_id, grant=grant)

    def _from_reduced(self, object_type: str, object_id: str, principal: str, action_type: str):
        match object_type:
            case "TABLE":
                catalog, database, table = object_id.split(".")
                return Grant(
                    principal=principal, action_type=action_type, catalog=catalog, database=database, table=table
                )
            case "VIEW":
                catalog, database, view = object_id.split(".")
                return Grant(
                    principal=principal, action_type=action_type, catalog=catalog, database=database, view=view
                )
            case "DATABASE":
                catalog, database = object_id.split(".")
                return Grant(principal=principal, action_type=action_type, catalog=catalog, database=database)
            case "CATALOG":
                catalog = object_id
                return Grant(principal=principal, action_type=action_type, catalog=catalog)
            case "ANONYMOUS FUNCTION":
                catalog = object_id
                return Grant(principal=principal, action_type=action_type, catalog=catalog, anonymous_function=True)
            case "ANY FILE":
                catalog = object_id
                return Grant(principal=principal, action_type=action_type, catalog=catalog, any_file=True)

    def object_types(self) -> set[str]:
        return {"TABLE", "DATABASE", "VIEW", "CATALOG", "ANONYMOUS FUNCTION", "ANY FILE"}

    def get_apply_task(self, item: Permissions, migration_state: MigrationState):
        grant = Grant(**json.loads(item.raw))
        target_principal = migration_state.get_target_principal(grant.principal)
        if target_principal is None:
            # this is a grant for user, service principal, or irrelevant group
            return None
        target_grant = dataclasses.replace(grant, principal=target_principal)
        # this has to be executed on tacl cluster, otherwise - use SQLExecutionAPI backend & Warehouse
        return partial(self._apply_grant, target_grant)

    def _apply_grant(self, grant: Grant):
        for sql in grant.hive_grant_sql():
            self._sql_backend.execute(sql)
        return True
