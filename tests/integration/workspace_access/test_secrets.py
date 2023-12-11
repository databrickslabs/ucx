import logging
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.mapping import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.workspace import AclPermission

from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_permissions_for_secrets(ws: WorkspaceClient, make_group, make_secret_scope, make_secret_scope_acl):
    group_a = make_group()
    group_b = make_group()

    scope = make_secret_scope()
    make_secret_scope_acl(scope=scope, principal=group_a.display_name, permission=AclPermission.WRITE)

    scope_acl = ws.secrets.get_acl(scope, group_a.display_name)

    secret_support = SecretScopesSupport(ws)

    migration_state = MigrationState(
        [
            MigratedGroup.partial_info(group_a, group_b),
        ]
    )

    for crawler_task in secret_support.get_crawler_tasks():
        permission = crawler_task()
        apply_task = secret_support.get_apply_task(permission, migration_state)
        if not apply_task:
            continue
        apply_task()

    reflected_scope_acls = ws.secrets.get_acl(scope, group_b.display_name)

    assert reflected_scope_acls.principal == group_b.display_name
    assert scope_acl.permission == reflected_scope_acls.permission
