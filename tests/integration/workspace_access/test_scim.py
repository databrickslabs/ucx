from datetime import timedelta

import pytest
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam

from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.scim import ScimSupport

from . import apply_tasks


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_some_entitlements(
    acc: AccountClient,
    ws: WorkspaceClient,
    make_group,
    make_acc_group,
    use_permission_migration_api: bool,
):
    ws_group = make_group()
    acc_group = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), acc_group.id, permissions=[iam.WorkspacePermission.USER])
    migrated_group = MigratedGroup.partial_info(ws_group, acc_group)
    ws.groups.patch(
        ws_group.id,
        operations=[
            iam.Patch(
                op=iam.PatchOp.ADD,
                path="entitlements",
                value=[iam.ComplexValue(value="databricks-sql-access").as_dict()],
            )
        ],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )

    @retried(on=[AssertionError], timeout=timedelta(seconds=10))
    def assert_scim_permissions_with_retry(scim_support, group_id, entitlement) -> None:
        _, load_permissions = scim_support.load_for_group(group_id)
        # Note: the following assert is the source of the KeyError (and why we might need to re-load the permissions).
        assert entitlement in load_permissions

    scim_support = ScimSupport(ws)

    assert_scim_permissions_with_retry(scim_support, ws_group.id, "databricks-sql-access")

    if use_permission_migration_api:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(scim_support, [migrated_group])

    assert_scim_permissions_with_retry(scim_support, ws_group.id, "databricks-sql-access")


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
