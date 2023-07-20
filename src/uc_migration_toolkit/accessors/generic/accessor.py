from abc import abstractmethod
from collections.abc import Callable, Iterator
from typing import Generic, TypeVar

from databricks.sdk.service.iam import ObjectPermissions

from uc_migration_toolkit.accessors.generic.inventory_item import (
    PermissionsInventoryItem,
)
from uc_migration_toolkit.providers.client import ClientMixin

DatabricksObject = TypeVar("DatabricksObject")


class GenericAccessor(Generic[DatabricksObject], ClientMixin):
    @property
    @abstractmethod
    def object_type(self):
        return self.object_type

    @property
    @abstractmethod
    def listing_function(self) -> Callable[..., Iterator[DatabricksObject]]:
        """"""

    @property
    @abstractmethod
    def id_attribute(self) -> str:
        """"""

    def fetch_permissions(self, item: DatabricksObject) -> ObjectPermissions:
        """"""
        return self.ws_client.permissions.get(self.object_type, self.get_id(item))

    def get_id(self, item: DatabricksObject):
        return item.__getattribute__(self.id_attribute)

    def get_inventory_item(self, item: DatabricksObject) -> PermissionsInventoryItem:
        return PermissionsInventoryItem(
            object_id=self.get_id(item),
            object_type=self.object_type,
            object_info=item.as_dict(),
            object_permissions=self.fetch_permissions(item).as_dict(),
        )
