from dataclasses import dataclass
from functools import partial

from databricks.sdk.service.iam import AccessControlRequest

from uc_migration_toolkit.managers.group import GroupPairs
from uc_migration_toolkit.managers.inventory.inventorizer import StandardInventorizer
from uc_migration_toolkit.managers.inventory.table import InventoryTableManager
from uc_migration_toolkit.managers.inventory.types import (
    LogicalObjectType,
    PermissionsInventoryItem,
    RequestObjectType,
)
from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.utils import ThreadedExecution


@dataclass
class PermissionRequestPayload:
    request_object_type: RequestObjectType
    object_id: str
    access_control_list: list[AccessControlRequest]

    def as_dict(self):
        return {
            "request_object_type": self.request_object_type,
            "object_id": self.object_id,
            "access_control_list": self.access_control_list,
        }


class PermissionManager:
    def __init__(self, inventory_table_manager: InventoryTableManager):
        self.config = config_provider.config
        self.inventory_table_manager = inventory_table_manager

    @staticmethod
    def get_inventorizers():
        return [
            StandardInventorizer(
                logical_object_type=LogicalObjectType.CLUSTER,
                request_object_type=RequestObjectType.CLUSTERS,
                listing_function=provider.ws.clusters.list,
                id_attribute="cluster_id",
            )
        ]

    def inventorize_permissions(self):
        logger.info("Inventorizing the permissions")

        for inventorizer in self.get_inventorizers():
            inventorizer.preload()
            collected = inventorizer.inventorize()
            if collected:
                self.inventory_table_manager.save(collected)
            else:
                logger.warning(f"No objects of type {inventorizer.logical_object_type} were found")

        logger.info("Permissions were inventorized and saved")

    @staticmethod
    def get_destination_by_source(source_name: str, pairs: GroupPairs) -> str | None:
        return next((p.destination.display_name for p in pairs if p.source.display_name == source_name), None)

    def prepare_new_permission_request(
            self, item: PermissionsInventoryItem, pairs: GroupPairs
    ) -> PermissionRequestPayload:
        new_acls: list[AccessControlRequest] = []

        logger.info("Attempting to build the new ACLs, verifying if there are any relevant groups")
        for acl in item.typed_object_permissions.access_control_list:
            if acl.group_name in [p.source.display_name for p in pairs]:
                destination = self.get_destination_by_source(acl.group_name, pairs)
                if not destination:
                    msg = f"Destination group for {acl.group_name} was not found"
                    raise RuntimeError(msg)

                logger.info(f"Found permissions relevant for group {acl.group_name} (target group: {destination})")
                for permission in acl.all_permissions:
                    if permission.inherited:
                        continue
                    new_acls.append(
                        AccessControlRequest(
                            group_name=destination,
                            permission_level=permission.permission_level,
                        )
                    )
            else:
                continue

        if new_acls:
            return PermissionRequestPayload(
                request_object_type=item.request_object_type,
                object_id=item.object_id,
                access_control_list=new_acls,
            )

    @staticmethod
    def apply_permissions_in_parallel(requests: list[PermissionRequestPayload]):
        logger.info("Applying the permissions in parallel")
        executables = [
            partial(
                provider.ws.permissions.update,
                request_object_type=payload.request_object_type,
                request_object_id=payload.object_id,
                access_control_list=payload.access_control_list,
            )
            for payload in requests
        ]
        execution = ThreadedExecution[None](executables)
        execution.run()
        logger.info("All permissions were applied")

    def apply_backup_group_permissions(self, pairs: GroupPairs):
        logger.info("Applying the permissions to the backup groups")
        permissions_on_source = self.inventory_table_manager.load_for_groups(groups=[p.source for p in pairs])
        applicable_permissions = filter(
            lambda item: item is not None,
            [self.prepare_new_permission_request(item, pairs) for item in permissions_on_source],
        )
        self.apply_permissions_in_parallel(requests=list(applicable_permissions))
        logger.info("Permissions were applied")

    def apply_account_group_permissions(self):
        logger.info("Applying workspace-level permissions to the account-level groups")
        logger.info("Permissions were successfully applied to the account-level groups")
