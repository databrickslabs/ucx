import logging
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import AclPermission

from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport

logger = logging.getLogger(__name__)

def test_secrets(
    ws: WorkspaceClient,
    make_ucx_group,
    sql_backend,
    inventory_schema,
    make_random,
    make_secret_scope,
    make_secret_scope_acl,
):
    """
    This test does the following:
    * creates 10 ws groups and secret scopes  with permissions to
    * assigns ACL permissions to the groups on the secrets
    * run assessment
    * migrate this group
    * verify that the migrated group has the same permissions on the scopes
    :return:
    """

    created_groups = []
    created_secrets_scopes = []
    created_items = []
    for i in range(10):
        ws_group, acc_group = make_ucx_group()
        created_groups.append(ws_group)

        scope = make_secret_scope()
        created_secrets_scopes.append(scope)
        created_items.append((ws_group, acc_group, scope))
        make_secret_scope_acl(scope=scope, principal=ws_group.display_name, permission=AclPermission.WRITE)

        scope_acls = ws.secrets.get_acl(scope, ws_group.display_name)
        logger.debug(f"secret scope acls: {scope_acls}")

    created_groups_names = [g.display_name for g in created_groups]

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
        ws_group = item[0]
        acc_group = item[1]
        scope = item[2]
        old_group = ws.groups.get(ws_group.id)
        reflected_group = ws.groups.get(acc_group.id)
        scope_acls = ws.secrets.get_acl(scope, reflected_group.display_name)
        logger.debug(f"old_group: {old_group}")
        logger.debug(f"reflected_group: {reflected_group}")
        logger.debug(f"secret scope: {scope}")
        logger.debug(f"secret scope acls: {scope_acls}")

        assert reflected_group.display_name == ws_group.display_name
        assert reflected_group.id == acc_group.id
        assert len(reflected_group.members) == len(old_group.members)
