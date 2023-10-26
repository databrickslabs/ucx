from unittest.mock import MagicMock

from databricks.sdk.core import DatabricksError
from databricks.sdk.service import iam
from databricks.sdk.service.iam import Group

from databricks.labs.ucx.workspace_access.scim import ScimSupport


def test_applier_task_should_return_true_if_roles_are_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", roles=[iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")])
    sup = ScimSupport(ws=ws)

    result = sup._applier_task(group_id="1", value=[iam.ComplexValue(value="role1")], property_name="roles")
    assert result


def test_applier_task_should_return_true_if_entitlements_are_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(
        id="1", roles=[iam.ComplexValue(value="role1")], entitlements=[iam.ComplexValue(value="allow-cluster-create")]
    )
    sup = ScimSupport(ws=ws)

    result = sup._applier_task(
        group_id="1", value=[iam.ComplexValue(value="allow-cluster-create")], property_name="entitlements"
    )
    assert result


def test_applier_task_should_return_false_if_roles_are_not_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", roles=[iam.ComplexValue(value="role2")])
    sup = ScimSupport(ws=ws)

    result = sup._applier_task(group_id="1", value=[iam.ComplexValue(value="role1")], property_name="roles")
    assert not result


def test_safe_patch_group_when_error_non_retriable():
    ws = MagicMock()
    ws.groups.patch.side_effect = DatabricksError(error_code="PERMISSION_DENIED")
    sup = ScimSupport(ws=ws)
    operations = [
        iam.Patch(op=iam.PatchOp.ADD, path="roles", value=[e.as_dict() for e in [iam.ComplexValue(value="role1")]])
    ]
    schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
    result = sup._safe_patch_group(id="1", operations=operations, schemas=schemas)
    assert result is None


def test_safe_get_group_when_error_non_retriable():
    ws = MagicMock()
    ws.groups.get.side_effect = DatabricksError(error_code="PERMISSION_DENIED")
    sup = ScimSupport(ws=ws)
    result = sup._safe_get_group(id="1")
    assert result is None
