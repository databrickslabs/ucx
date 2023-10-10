from unittest.mock import MagicMock, call

from databricks.sdk.service import iam
from databricks.sdk.service.iam import Group

from databricks.labs.ucx.workspace_access.scim import ScimSupport


def test_applier_task_should_return_true_if_roles_are_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", roles=[iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")])
    sup = ScimSupport(ws=ws)

    result = sup._applier_task(group_id="1", value=[iam.ComplexValue(value="role1")], property_name="roles")
    assert result


def test_applier_task_should_return_false_if_roles_are_not_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", roles=[iam.ComplexValue(value="role2")])
    sup = ScimSupport(ws=ws)

    result = sup._applier_task(group_id="1", value=[iam.ComplexValue(value="role1")], property_name="roles")
    assert not result


def test_applier_task_should_be_called_three_times_if_roles_are_not_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", roles=[iam.ComplexValue(value="role2")])
    sup = ScimSupport(ws=ws)

    sup._applier_task(group_id="1", value=[iam.ComplexValue(value="role1")], property_name="roles")
    assert len(ws.groups.patch.mock_calls) == 3
    assert ws.groups.patch.mock_calls == [
        call(
            id="1",
            operations=[iam.Patch(op=iam.PatchOp.ADD, path="roles", value=[{"value": "role1"}])],
            schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
        ),
        call(
            id="1",
            operations=[iam.Patch(op=iam.PatchOp.ADD, path="roles", value=[{"value": "role1"}])],
            schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
        ),
        call(
            id="1",
            operations=[iam.Patch(op=iam.PatchOp.ADD, path="roles", value=[{"value": "role1"}])],
            schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
        ),
    ]
    assert len(ws.groups.get.mock_calls) == 3
    assert ws.groups.get.mock_calls == [call("1"), call("1"), call("1")]
