import dataclasses

from databricks.sdk.service.workspace import AclItem as SdkAclItem
from databricks.sdk.service.workspace import AclPermission as SdkAclPermission

from databricks.labs.ucx.inventory.types import AclItemsContainer


def test_acl_items_container_serde():
    sdk_items = [
        SdkAclItem(principal="blah", permission=SdkAclPermission.READ),
        SdkAclItem(principal="blah2", permission=SdkAclPermission.WRITE),
    ]

    container = AclItemsContainer.from_sdk(sdk_items)

    after = container.to_sdk()

    assert after == sdk_items

    _dump = dataclasses.asdict(container)
    _str = AclItemsContainer.from_dict(_dump)

    assert _str == container
