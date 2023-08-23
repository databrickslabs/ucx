from databricks.sdk.service.workspace import AclItem as SdkAclItem
from databricks.sdk.service.workspace import AclPermission as SdkAclPermission
from pydantic.tools import parse_obj_as


def test_acl_items_container_serde():
    sdk_items = [
        SdkAclItem(principal="blah", permission=SdkAclPermission.READ),
        SdkAclItem(principal="blah2", permission=SdkAclPermission.WRITE),
    ]

    container = AclItemsContainer.from_sdk(sdk_items)

    after = container.to_sdk()

    assert after == sdk_items

    _dump = container.model_dump(mode="json")
    _str = parse_obj_as(AclItemsContainer, _dump)

    assert _str == container
