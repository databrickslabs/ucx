import logging
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.mapping import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.workspace import AclPermission

from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_secrets(
    ws: WorkspaceClient,
    make_ucx_group,
    sql_backend,
    inventory_schema,
    make_secret_scope,
    make_secret_scope_acl,
):
    """
    This test does the following:
    * creates 2 ws groups and secret scopes
    * assigns ACL permissions to the groups on the secrets
    * run assessment
    * migrate groups
    * verify that the migrated groups has the same permissions on the scopes
    :return:
    """

    created_items = []
    for _ in range(2):
        ws_group, acc_group = make_ucx_group()

        scope = make_secret_scope()
        make_secret_scope_acl(scope=scope, principal=ws_group.display_name, permission=AclPermission.WRITE)

        scope_acl = ws.secrets.get_acl(scope, ws_group.display_name)
        item = {
            "ws_group": ws_group,
            "acc_group": acc_group,
            "scope": scope,
            "scope_acl": scope_acl,
        }
        created_items.append(item)
        logger.debug(f"secret scope acls: {scope_acl}")

    created_groups_names = [i["ws_group"].display_name for i in created_items]

    # Task 1 - crawl_permissions
    secret_support = SecretScopesSupport(ws)
    pi = PermissionManager(sql_backend, inventory_schema, [secret_support])
    pi.cleanup()
    pi.inventorize_permissions()

    # Task 2 - crawl_groups
    group_manager = GroupManager(sql_backend, ws, inventory_schema, include_group_names=created_groups_names)
    group_manager.snapshot()

    # Task 2 - apply_permissions_to_backup_groups
    group_manager = GroupManager(sql_backend, ws, inventory_schema, include_group_names=created_groups_names)
    group_manager.rename_groups()

    # Task 3 - reflect_account_groups_on_workspace
    group_manager = GroupManager(sql_backend, ws, inventory_schema, include_group_names=created_groups_names)
    group_manager.reflect_account_groups_on_workspace()

    # Task 4 - apply_permissions_to_account_groups
    group_manager = GroupManager(sql_backend, ws, inventory_schema, include_group_names=created_groups_names)
    migration_state = group_manager.get_migration_state()
    permission_manager = PermissionManager.factory(ws, sql_backend, inventory_database=inventory_schema)
    permission_manager.apply_group_permissions(migration_state)

    for item in created_items:
        ws_group = item["ws_group"]
        acc_group = item["acc_group"]
        scope = item["scope"]
        old_scope_acl = item["scope_acl"]
        old_group = ws.groups.get(ws_group.id)
        reflected_group = ws.groups.get(acc_group.id)
        reflected_scope_acls = ws.secrets.get_acl(scope, reflected_group.display_name)

        assert reflected_group.display_name == ws_group.display_name
        assert reflected_group.id == acc_group.id
        assert old_scope_acl == reflected_scope_acls
        assert len(reflected_group.members) == len(old_group.members)
