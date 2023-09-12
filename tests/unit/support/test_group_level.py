import json
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service import iam

from databricks.labs.ucx.inventory.types import PermissionsInventoryItem
from databricks.labs.ucx.support.group_level import ScimSupport


def test_scim_crawler():
    ws = MagicMock()
    ws.groups.list.return_value = [
        iam.Group(
            id="1",
            display_name="group1",
            roles=[],  # verify that empty roles and entitlements are not returned
        ),
        iam.Group(
            id="2",
            display_name="group2",
            roles=[iam.ComplexValue(value="role1")],
            entitlements=[iam.ComplexValue(value="entitlement1")],
        ),
        iam.Group(
            id="3",
            display_name="group3",
            roles=[iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")],
            entitlements=[],
        ),
    ]
    sup = ScimSupport(ws=ws)
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 3
    ws.groups.list.assert_called_once()
    for task in tasks:
        item = task()
        if item.object_id == "1":
            assert item is None
        else:
            assert item.object_id in ["2", "3"]
            assert item.support in ["roles", "entitlements"]
            assert item.raw_object_permissions is not None


def test_scim_apply(migration_state):
    ws = MagicMock()
    sup = ScimSupport(ws=ws)
    sample_permissions = [iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")]
    item = PermissionsInventoryItem(
        object_id="test-ws",
        support="roles",
        raw_object_permissions=json.dumps([p.as_dict() for p in sample_permissions]),
    )

    task = sup.get_apply_task(item, migration_state, "backup")
    task()
    ws.groups.patch.assert_called_once_with(
        id="test-backup",
        operations=[iam.Patch(op=iam.PatchOp.ADD, path="roles", value=[p.as_dict() for p in sample_permissions])],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


def test_no_group_in_migration_state(migration_state):
    ws = MagicMock()
    sup = ScimSupport(ws=ws)
    sample_permissions = [iam.ComplexValue(value="role1"), iam.ComplexValue(value="role2")]
    item = PermissionsInventoryItem(
        object_id="test-non-existent",
        support="roles",
        raw_object_permissions=json.dumps([p.as_dict() for p in sample_permissions]),
    )
    with pytest.raises(ValueError):
        sup._get_apply_task(item, migration_state, "backup")


def test_non_relevant(migration_state):
    ws = MagicMock()
    sup = ScimSupport(ws=ws)
    sample_permissions = [iam.ComplexValue(value="role1")]
    relevant_item = PermissionsInventoryItem(
        object_id="test-ws",
        support="roles",
        raw_object_permissions=json.dumps([p.as_dict() for p in sample_permissions]),
    )
    irrelevant_item = PermissionsInventoryItem(
        object_id="something-non-relevant",
        support="roles",
        raw_object_permissions=json.dumps([p.as_dict() for p in sample_permissions]),
    )
    assert sup.is_item_relevant(relevant_item, migration_state)
    assert not sup.is_item_relevant(irrelevant_item, migration_state)
