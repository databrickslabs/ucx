from dataclasses import dataclass
from functools import partial
from typing import Literal

from databricks.sdk.service.iam import AccessControlRequest, Group

from uc_migration_toolkit.managers.group import MigrationGroupsProvider
from uc_migration_toolkit.managers.inventory.inventorizer import StandardInventorizer
from uc_migration_toolkit.managers.inventory.listing import CustomListing
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
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.INSTANCE_POOL,
                request_object_type=RequestObjectType.INSTANCE_POOLS,
                listing_function=provider.ws.instance_pools.list,
                id_attribute="instance_pool_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.CLUSTER_POLICY,
                request_object_type=RequestObjectType.CLUSTER_POLICIES,
                listing_function=provider.ws.cluster_policies.list,
                id_attribute="policy_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.PIPELINE,
                request_object_type=RequestObjectType.PIPELINES,
                listing_function=provider.ws.pipelines.list_pipelines,
                id_attribute="pipeline_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.JOB,
                request_object_type=RequestObjectType.JOBS,
                listing_function=provider.ws.jobs.list,
                id_attribute="job_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.EXPERIMENT,
                request_object_type=RequestObjectType.EXPERIMENTS,
                listing_function=provider.ws.experiments.list_experiments,
                id_attribute="experiment_id",
            ),
            StandardInventorizer(
                logical_object_type=LogicalObjectType.MODEL,
                request_object_type=RequestObjectType.REGISTERED_MODELS,
                listing_function=CustomListing.list_models,
                id_attribute="id",
            ),
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
    def _prepare_new_permission_request(
        item: PermissionsInventoryItem,
        migration_groups_provider: MigrationGroupsProvider,
        destination: Literal["backup", "account"],
    ) -> PermissionRequestPayload:
        new_acls: list[AccessControlRequest] = []

        for acl in item.typed_object_permissions.access_control_list:
            if acl.group_name in [g.workspace.display_name for g in migration_groups_provider.groups]:
                migration_info = migration_groups_provider.get_by_workspace_group_name(acl.group_name)
                assert migration_info is not None, f"Group {acl.group_name} is not in the migration groups provider"
                destination_group: Group = getattr(migration_info, destination)
                for permission in acl.all_permissions:
                    if permission.inherited:
                        continue
                    new_acls.append(
                        AccessControlRequest(
                            group_name=destination_group.display_name,
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
    def _apply_permissions_in_parallel(requests: list[PermissionRequestPayload]):
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

    def apply_group_permissions(
        self, migration_groups_provider: MigrationGroupsProvider, destination: Literal["backup", "account"]
    ):
        logger.info(f"Applying the permissions to {destination} groups")
        logger.info(f"Total groups to apply permissions: {len(migration_groups_provider.groups)}")

        permissions_on_source = self.inventory_table_manager.load_for_groups(
            groups=[g.workspace for g in migration_groups_provider.groups]
        )
        applicable_permissions = list(
            filter(
                lambda item: item is not None,
                [
                    self._prepare_new_permission_request(item, migration_groups_provider, destination=destination)
                    for item in permissions_on_source
                ],
            )
        )

        logger.info(f"Applying {len(applicable_permissions)} permissions")

        self._apply_permissions_in_parallel(requests=applicable_permissions)
        logger.info("All permissions were applied")
