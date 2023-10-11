import pytest
from databricks.sdk.service import iam

from databricks.labs.ucx.workspace_access.scim import ScimSupport
import pytest
from databricks.sdk.service import iam
from databricks.labs.ucx.mixins.fixtures import ws, make_query, make_user, make_group
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState, MigrationGroupInfo
from databricks.labs.ucx.workspace_access.redash import SqlPermissionsSupport
from databricks.labs.ucx.workspace_access import generic, redash, scim, secrets
from databricks.sdk.service import sql
from databricks.sdk.service.sql import ObjectTypePlural


def test_applier_task_should_apply_proper_entitlements_to_ws_groups(ws, make_ucx_group):
    ws_group, acc_group = make_ucx_group()
    sup = ScimSupport(ws=ws)

    ws_result = sup._applier_task(
        group_id=ws_group.id, value=[iam.ComplexValue(value="databricks-sql-access")], property_name="entitlements"
    )
    assert ws_result


@pytest.mark.skip("Implement integration testing with roles")
def test_applier_task_should_apply_proper_roles(ws, make_ucx_group):
    ws_group, acc_group = make_ucx_group()
    sup = ScimSupport(ws=ws)

    result = sup._applier_task(group_id=ws_group.id, value=[iam.ComplexValue(value="user")], property_name="roles")
    assert result



def test_scim_support_should_replicate_entitlement_to_backup_group(ws, make_ucx_group, make_group):
    #Ws group have allow-cluster-create by default, we want to replicate that
    ws_group, acc_group = make_ucx_group()
    backup_group_name = ws_group.display_name + "-backup"
    backup_group = make_group(display_name=backup_group_name)

    migration_state = GroupMigrationState()
    migration_state.add(
        group=MigrationGroupInfo(
            workspace=iam.Group(display_name=ws_group.display_name, id=ws_group.id),
            account=iam.Group(display_name=acc_group.display_name, id=acc_group.id),
            backup=iam.Group(display_name=backup_group_name, id=backup_group.id),
        )
    )

    sup = ScimSupport(ws=ws)

    tasks = list(sup.get_crawler_tasks())
    # Crawler tasks should return the users, admins, and the ws_group as there are no other groups present
    assert len(tasks) == 3
    applied_tasks = []
    for task in tasks:
        apply_task = sup.get_apply_task(task(), migration_state, "backup")
        applied_tasks.append(apply_task())

    # Validate that no errors has been thrown when applying permission to backup group
    applied_tasks = [task for task in applied_tasks if task is not None]
    assert len(applied_tasks) == 1

    backup_group_entitlement = ws.groups.get(backup_group.id).entitlements

    # Validate that allow-cluster-create entitlement has been applied properly to the backup group
    assert len(backup_group_entitlement) == 1
    assert backup_group_entitlement == [iam.ComplexValue(display=None, primary=None, type=None, value='allow-cluster-create')]


