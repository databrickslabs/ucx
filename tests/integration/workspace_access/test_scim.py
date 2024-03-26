from datetime import timedelta

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam

from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.scim import ScimSupport

from . import apply_tasks


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_some_entitlements(
    ws: WorkspaceClient,
    make_group,
    make_acc_ws_group,
    permission_manager: PermissionManager,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_ws_group()
    ws.groups.patch(
        group_a.id,
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path="entitlements",
                value=[iam.ComplexValue(value="databricks-sql-access").as_dict()],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )

    scim_support = ScimSupport(ws)
    _, before = scim_support.load_for_group(group_a.id)
    assert "databricks-sql-access" in before

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]
    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(scim_support, migrated_groups)

    _, after = scim_support.load_for_group(group_b.id)
    assert "databricks-sql-access" in after


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_verify_entitlements(ws, make_group):
    group_a = make_group()
    ws.groups.patch(
        group_a.id,
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path="entitlements",
                value=[iam.ComplexValue(value="databricks-sql-access").as_dict()],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )

    item = Permissions(object_id=group_a.id, object_type="entitlements", raw='[{"value": "databricks-sql-access"}]')

    scim_support = ScimSupport(ws)
    task = scim_support.get_verify_task(item)
    result = task()

    assert result
