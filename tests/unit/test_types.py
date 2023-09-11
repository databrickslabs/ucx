from databricks.labs.ucx.inventory.types import (
    PermissionsInventoryItem,
    RequestObjectType,
)


def test_item_without_extras():
    sample_item = PermissionsInventoryItem(
        object_id="object1",
        support="clusters",
        raw_object_permissions="test acl",
    )
    assert sample_item.extras() == {}


def test_item_with_extras():
    sample_item = PermissionsInventoryItem(
        object_id="object1", support="clusters", raw_object_permissions="test acl", raw_extras='{"test": "test"}'
    )
    assert sample_item.extras() == {"test": "test"}


def test_request_object_type():
    typed = RequestObjectType.AUTHORIZATION
    assert typed == "authorization"
    assert typed.__repr__() == "authorization"
