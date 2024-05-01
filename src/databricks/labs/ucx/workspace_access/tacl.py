import collections
import dataclasses
import functools
import json
from collections.abc import Callable, Iterator
from datetime import timedelta
from functools import partial

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore.grants import Grant, GrantsCrawler
from databricks.labs.ucx.workspace_access.base import AclSupport, Permissions, StaticListing
from databricks.labs.ucx.workspace_access.groups import MigrationState


class TableAclSupport(AclSupport):
    def __init__(
        self,
        grants_crawler: GrantsCrawler,
        sql_backend: SqlBackend,
        verify_timeout: timedelta | None = timedelta(minutes=1),
        # this parameter is for testing scenarios only - [{object_type}:{object_id}]
        # it will use StaticListing class to return only object ids that has the same object type
        include_object_permissions: list[str] | None = None,
    ):
        self._grants_crawler = grants_crawler
        self._sql_backend = sql_backend
        self._verify_timeout = verify_timeout
        self._include_object_permissions = include_object_permissions

    def get_crawler_tasks(self) -> Iterator[Callable[..., Permissions | None]]:
        # Table ACL permissions (grant/revoke and ownership) are not atomic. When granting the permissions,
        # the service would first get all existing permissions, append with the new permissions,
        # and set the full list in the database. If there are concurrent grant requests,
        # both requests might succeed and emit the audit logs, but what actually happens could be that
        # the new permission list from one request overrides the other one, causing permissions loss.
        # More info here: https://databricks.atlassian.net/browse/ES-908737
        #
        # Below optimization mitigates the issue by folding all action types (grants and own permissions)
        # for the same principal, object_id and object_type into one grant with comma separated list of action types.
        #
        # For example, the following table grants:
        # * GRANT SELECT ON TABLE hive_metastore.db_a.table_a TO group_a
        # * GRANT MODIFY ON TABLE hive_metastore.db_a.table_a TO group_a
        # will be folded and executed in one statement/transaction:
        # * GRANT SELECT, MODIFY ON TABLE hive_metastore.db_a.table_a TO group_a
        # The exception is the "OWN" permission which must be set using a separate ALTER statement.

        include_objects = set[tuple[str, str]]()
        if self._include_object_permissions:
            for item in StaticListing(self._include_object_permissions, self.object_types()):
                include_objects.add((item.object_type, item.object_id))

        folded_actions = collections.defaultdict(set)
        for grant in self._grants_crawler.snapshot():
            type_and_key = grant.this_type_and_key()
            if include_objects and type_and_key not in include_objects:
                continue
            key = (grant.principal, type_and_key)
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
            case "FUNCTION":
                catalog, database, udf = object_id.split(".")
                return Grant(principal=principal, action_type=action_type, catalog=catalog, database=database, udf=udf)
            case "ANONYMOUS FUNCTION":
                catalog = object_id
                return Grant(principal=principal, action_type=action_type, catalog=catalog, anonymous_function=True)
            case "ANY FILE":
                catalog = object_id
                return Grant(principal=principal, action_type=action_type, catalog=catalog, any_file=True)

    def object_types(self) -> set[str]:
        return {"TABLE", "DATABASE", "VIEW", "CATALOG", "FUNCTION", "ANONYMOUS FUNCTION", "ANY FILE"}

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
        """
        Apply grants and OWN permission serially for the same principal, object_id and object_type.
        Executing grants (using GRANT statement) and OWN permission (using ALTER statement) concurrently
        could cause permission lost due to limitations in the Table ACLs.
        More info here: https://databricks.atlassian.net/browse/ES-976290
        """
        for sql in grant.hive_grant_sql():
            self._sql_backend.execute(sql)

        object_type, object_id = grant.this_type_and_key()
        retry_on_value_error = retried(on=[NotFound], timeout=self._verify_timeout)
        retried_check = retry_on_value_error(self._verify)
        return retried_check(object_type, object_id, grant)

    def _verify(self, object_type: str, object_id: str, acl: Grant) -> bool:
        grant_dict = dataclasses.asdict(acl)
        del grant_dict["action_type"]
        del grant_dict["principal"]
        grants_on_object = self._grants_crawler.grants(**grant_dict)

        if grants_on_object:
            on_current_principal = [grant.action_type for grant in grants_on_object if grant.principal == acl.principal]
            acl_action_types = acl.action_type.split(", ")
            if all(action_type in on_current_principal for action_type in acl_action_types):
                return True
            msg = (
                f"Couldn't find permission for object type {object_type}, id {object_id} and principal {acl.principal}\n"
                f"acl to be applied={acl_action_types}\n"
                f"acl found in the object={on_current_principal}\n"
            )
            raise NotFound(msg)
        return False

    def get_verify_task(self, item: Permissions) -> Callable[[], bool]:
        grant = Grant(**json.loads(item.raw))
        return partial(self._verify, item.object_type, item.object_id, grant)
