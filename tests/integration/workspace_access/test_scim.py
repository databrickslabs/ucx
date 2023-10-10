
import pytest
from databricks.sdk.service import iam

from databricks.labs.ucx.workspace_access.scim import ScimSupport


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
