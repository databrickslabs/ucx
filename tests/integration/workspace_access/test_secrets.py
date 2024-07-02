import json
import logging
from datetime import timedelta

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import NotFound
from databricks.sdk.service import workspace
from databricks.sdk.service.workspace import AclPermission

from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigrationState
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport

from ..retries import retried
from . import apply_tasks

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_permissions_for_secrets(
    ws: WorkspaceClient,
    migrated_group,
    make_secret_scope,
    make_secret_scope_acl,
    use_permission_migration_api: bool,
):

    scope = make_secret_scope()
    make_secret_scope_acl(scope=scope, principal=migrated_group.name_in_workspace, permission=AclPermission.WRITE)

    scope_acl = ws.secrets.get_acl(scope, migrated_group.name_in_workspace)

    secret_support = SecretScopesSupport(ws)

    if use_permission_migration_api:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(secret_support, [migrated_group])

    reflected_scope_acls = ws.secrets.get_acl(scope, migrated_group.name_in_account)

    assert reflected_scope_acls.principal == migrated_group.name_in_account
    assert scope_acl.permission == reflected_scope_acls.permission


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_verify_permissions_for_secrets(ws: WorkspaceClient, make_group, make_secret_scope, make_secret_scope_acl):
    group_a = make_group()

    scope = make_secret_scope()
    make_secret_scope_acl(scope=scope, principal=group_a.display_name, permission=AclPermission.WRITE)

    item = Permissions(
        object_id=scope,
        object_type="secrets",
        raw=json.dumps(
            [
                workspace.AclItem(
                    principal=group_a.display_name,
                    permission=workspace.AclPermission.WRITE,
                ).as_dict()
            ]
        ),
    )

    secret_support = SecretScopesSupport(ws)
    task = secret_support.get_verify_task(item)
    result = task()

    assert result
