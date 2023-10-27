from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import iam
from databricks.sdk.service.iam import Group

from databricks.labs.ucx.workspace_access.generic import RetryableError
from databricks.labs.ucx.workspace_access.scim import ScimSupport


def test_applier_task_should_return_true_if_roles_are_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", roles=[iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")])
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    result = sup._applier_task(group_id="1", value=[iam.ComplexValue(value="role1")], property_name="roles")
    assert result


def test_applier_task_should_return_true_if_entitlements_are_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(
        id="1", roles=[iam.ComplexValue(value="role1")], entitlements=[iam.ComplexValue(value="allow-cluster-create")]
    )
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    result = sup._applier_task(
        group_id="1", value=[iam.ComplexValue(value="allow-cluster-create")], property_name="entitlements"
    )
    assert result


def test_applier_task_should_return_false_if_roles_are_not_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", roles=[iam.ComplexValue(value="role2")])
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    with pytest.raises(TimeoutError) as e:
        sup._applier_task(group_id="1", value=[iam.ComplexValue(value="role1")], property_name="roles")
    assert "Timed out after" in str(e.value)


def test_applier_task_should_return_false_if_entitlements_are_not_properly_applied():
    ws = MagicMock()
    ws.groups.get.return_value = Group(id="1", entitlements=[iam.ComplexValue(value="allow-cluster-create")])
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))

    with pytest.raises(TimeoutError) as e:
        sup._applier_task(
            group_id="1", value=[iam.ComplexValue(value="forbidden-cluster-create")], property_name="entitlements"
        )
    assert "Timed out after" in str(e.value)


def test_safe_patch_group_when_error_non_retriable():
    ws = MagicMock()
    ws.groups.patch.side_effect = DatabricksError(error_code="PERMISSION_DENIED")
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))
    operations = [
        iam.Patch(op=iam.PatchOp.ADD, path="roles", value=[e.as_dict() for e in [iam.ComplexValue(value="role1")]])
    ]
    schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
    result = sup._safe_patch_group(group_id="1", operations=operations, schemas=schemas)
    assert result is None


def test_safe_patch_group_when_error_retriable():
    ws = MagicMock()
    error_code = "INTERNAL_SERVER_ERROR"
    ws.groups.patch.side_effect = DatabricksError(error_code=error_code)
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))
    operations = [
        iam.Patch(op=iam.PatchOp.ADD, path="roles", value=[e.as_dict() for e in [iam.ComplexValue(value="role1")]])
    ]
    schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
    with pytest.raises(RetryableError) as e:
        sup._safe_patch_group(group_id="1", operations=operations, schemas=schemas)
    assert error_code in str(e)


def test_safe_get_group_when_error_non_retriable():
    ws = MagicMock()
    ws.groups.get.side_effect = DatabricksError(error_code="PERMISSION_DENIED")
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))
    result = sup._safe_get_group(group_id="1")
    assert result is None


def test_safe_get_group_when_error_retriable():
    ws = MagicMock()
    error_code = "INTERNAL_SERVER_ERROR"
    ws.groups.get.side_effect = DatabricksError(error_code=error_code)
    sup = ScimSupport(ws=ws, verify_timeout=timedelta(seconds=1))
    with pytest.raises(RetryableError) as e:
        sup._safe_get_group(group_id="1")
    assert error_code in str(e)
