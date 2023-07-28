import random
import time
from copy import deepcopy
from dataclasses import dataclass
from functools import partial
from typing import Literal

from databricks.sdk.service.iam import AccessControlRequest, Group, ObjectPermissions
from databricks.sdk.service.workspace import AclItem as SdkAclItem
from tenacity import retry, stop_after_attempt, wait_fixed, wait_random

from uc_migration_toolkit.managers.group import MigrationGroupsProvider
from uc_migration_toolkit.managers.inventory.inventorizer import (
    SecretScopeInventorizer,
    StandardInventorizer,
    TokensAndPasswordsInventorizer,
    WorkspaceInventorizer,
)
from uc_migration_toolkit.managers.inventory.listing import CustomListing
from uc_migration_toolkit.managers.inventory.table import InventoryTableManager
from uc_migration_toolkit.managers.inventory.types import (
    AclItemsContainer,
    LogicalObjectType,
    PermissionsInventoryItem,
    RequestObjectType,
)
from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.utils import ThreadedExecution, safe_get_acls


@dataclass
class PermissionRequestPayload:
    logical_object_type: LogicalObjectType
    request_object_type: RequestObjectType | None
    object_id: str
    access_control_list: list[AccessControlRequest]


@dataclass
class SecretsPermissionRequestPayload:
    object_id: str
    access_control_list: list[SdkAclItem]


class PermissionManager:
    def __init__(self, inventory_table_manager: InventoryTableManager):
        self.config = config_provider.config
        self.inventory_table_manager = inventory_table_manager

    @staticmethod
    def get_inventorizers():
        return [
            TokensAndPasswordsInventorizer(),
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
            StandardInventorizer(
                logical_object_type=LogicalObjectType.WAREHOUSE,
                request_object_type=RequestObjectType.SQL_WAREHOUSES,
                listing_function=provider.ws.warehouses.list,
                id_attribute="id",
            ),
            SecretScopeInventorizer(),
            WorkspaceInventorizer(),
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
    def __prepare_request_for_permissions_api(
        item: PermissionsInventoryItem,
        migration_groups_provider: MigrationGroupsProvider,
        destination: Literal["backup", "account"],
    ) -> PermissionRequestPayload:
        _existing_permissions: ObjectPermissions = item.typed_object_permissions
        _acl = _existing_permissions.access_control_list
        acl_requests = []

        for _item in _acl:
            if _item.group_name in [g.workspace.display_name for g in migration_groups_provider.groups]:
                migration_info = migration_groups_provider.get_by_workspace_group_name(_item.group_name)
                assert migration_info is not None, f"Group {_item.group_name} is not in the migration groups provider"
                destination_group: Group = getattr(migration_info, destination)
                _item.group_name = destination_group.display_name
                _reqs = [
                    AccessControlRequest(
                        group_name=_item.group_name,
                        service_principal_name=_item.service_principal_name,
                        user_name=_item.user_name,
                        permission_level=p.permission_level,
                    )
                    for p in _item.all_permissions
                    if not p.inherited
                ]
                acl_requests.extend(_reqs)

        return PermissionRequestPayload(
            logical_object_type=item.logical_object_type,
            request_object_type=item.request_object_type,
            object_id=item.object_id,
            access_control_list=acl_requests,
        )

    @staticmethod
    def _prepare_permission_request_for_secrets_api(
        item: PermissionsInventoryItem,
        migration_groups_provider: MigrationGroupsProvider,
        destination: Literal["backup", "account"],
    ) -> SecretsPermissionRequestPayload:
        _existing_acl_container: AclItemsContainer = item.typed_object_permissions
        _final_acls = []

        logger.debug("Preparing the permissions for the secrets API")

        for _existing_acl in _existing_acl_container.acls:
            _new_acl = deepcopy(_existing_acl)

            if _existing_acl.principal in [g.workspace.display_name for g in migration_groups_provider.groups]:
                migration_info = migration_groups_provider.get_by_workspace_group_name(_existing_acl.principal)
                assert (
                    migration_info is not None
                ), f"Group {_existing_acl.principal} is not in the migration groups provider"
                destination_group: Group = getattr(migration_info, destination)
                _new_acl.principal = destination_group.display_name
                _final_acls.append(_new_acl)

        _typed_acl_container = AclItemsContainer(acls=_final_acls)

        return SecretsPermissionRequestPayload(
            object_id=item.object_id,
            access_control_list=_typed_acl_container.to_sdk(),
        )

    def _prepare_new_permission_request(
        self,
        item: PermissionsInventoryItem,
        migration_groups_provider: MigrationGroupsProvider,
        destination: Literal["backup", "account"],
    ) -> PermissionRequestPayload | SecretsPermissionRequestPayload:
        if isinstance(item.request_object_type, RequestObjectType) and isinstance(
            item.typed_object_permissions, ObjectPermissions
        ):
            return self.__prepare_request_for_permissions_api(item, migration_groups_provider, destination)
        elif item.logical_object_type == LogicalObjectType.SECRET_SCOPE:
            return self._prepare_permission_request_for_secrets_api(item, migration_groups_provider, destination)
        else:
            logger.warning(
                f"Unsupported permissions payload for object {item.object_id} "
                f"with logical type {item.logical_object_type}"
            )

    @staticmethod
    @retry(wait=wait_fixed(1) + wait_random(0, 2), stop=stop_after_attempt(5))
    def _scope_permissions_applicator(request_payload: SecretsPermissionRequestPayload):
        # TODO: rewrite and generalize this
        for _acl_item in request_payload.access_control_list:
            # this request will create OR update the ACL for the given principal
            # it means that the access_control_list should only keep records required for update
            provider.ws.secrets.put_acl(
                scope=request_payload.object_id, principal=_acl_item.principal, permission=_acl_item.permission
            )
            logger.debug(f"Applied new permissions for scope {request_payload.object_id}: {_acl_item}")
            # in-flight check for the applied permissions
            # the api might be inconsistent, therefore we need to check that the permissions were applied
            for _ in range(3):
                time.sleep(random.random() * 2)
                applied_acls = safe_get_acls(
                    provider.ws, scope_name=request_payload.object_id, group_name=_acl_item.principal
                )
                assert applied_acls, f"Failed to apply permissions for {_acl_item.principal}"
                assert applied_acls.permission == _acl_item.permission, (
                    f"Failed to apply permissions for {_acl_item.principal}. "
                    f"Expected: {_acl_item.permission}. Actual: {applied_acls.permission}"
                )

    @staticmethod
    def _standard_permissions_applicator(request_payload: PermissionRequestPayload):
        provider.ws.permissions.update(
            request_object_type=request_payload.request_object_type,
            request_object_id=request_payload.object_id,
            access_control_list=request_payload.access_control_list,
        )

    def applicator(self, request_payload: PermissionRequestPayload | SecretsPermissionRequestPayload):
        if isinstance(request_payload, PermissionRequestPayload):
            self._standard_permissions_applicator(request_payload)
        elif isinstance(request_payload, SecretsPermissionRequestPayload):
            self._scope_permissions_applicator(request_payload)
        else:
            logger.warning(f"Unsupported payload type {type(request_payload)}")

    def _apply_permissions_in_parallel(
        self, requests: list[PermissionRequestPayload | SecretsPermissionRequestPayload]
    ):
        executables = [partial(self.applicator, payload) for payload in requests]
        execution = ThreadedExecution[None](executables)
        execution.run()

    def apply_group_permissions(
        self, migration_groups_provider: MigrationGroupsProvider, destination: Literal["backup", "account"]
    ):
        logger.info(f"Applying the permissions to {destination} groups")
        logger.info(f"Total groups to apply permissions: {len(migration_groups_provider.groups)}")

        permissions_on_source = self.inventory_table_manager.load_for_groups(
            groups=[g.workspace.display_name for g in migration_groups_provider.groups]
        )
        permission_payloads: list[PermissionRequestPayload | SecretsPermissionRequestPayload] = [
            self._prepare_new_permission_request(item, migration_groups_provider, destination=destination)
            for item in permissions_on_source
        ]
        logger.info(f"Applying {len(permission_payloads)} permissions")

        self._apply_permissions_in_parallel(requests=permission_payloads)
        logger.info("All permissions were applied")
