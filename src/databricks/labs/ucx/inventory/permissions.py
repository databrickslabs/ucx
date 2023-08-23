import random
import time
from copy import deepcopy
from dataclasses import dataclass, replace
from functools import partial
from typing import Literal

from databricks.sdk.service.iam import AccessControlRequest, Group, ObjectPermissions
from databricks.sdk.service.workspace import AclItem as SdkAclItem
from tenacity import retry, stop_after_attempt, wait_fixed, wait_random

from databricks.labs.ucx.inventory.inventorizer import BaseInventorizer
from databricks.labs.ucx.inventory.workspace import (
    LogicalObjectType,
    RequestObjectType,
    WorkspaceInventory,
    WorkspacePermissions, RolesAndEntitlements,
)
from databricks.labs.ucx.providers.client import ImprovedWorkspaceClient
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.providers.logger import logger
from databricks.labs.ucx.utils import ThreadedExecution, safe_get_acls


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


@dataclass
class RolesAndEntitlementsRequestPayload:
    payload: RolesAndEntitlements
    group_id: str


AnyRequestPayload = PermissionRequestPayload | SecretsPermissionRequestPayload | RolesAndEntitlementsRequestPayload


# TODO: this class has too many @staticmethod and they must not be such. write a unit test for this logic.
class PermissionManager:
    def __init__(self, ws: ImprovedWorkspaceClient, workspace_inventory: WorkspaceInventory):
        self._ws = ws
        self._workspace_inventory = workspace_inventory
        self._inventorizers = []

    @property
    def inventorizers(self) -> list[BaseInventorizer]:
        return self._inventorizers

    def set_inventorizers(self, value: list[BaseInventorizer]):
        self._inventorizers = value

    def inventorize_permissions(self):
        for inventorizer in self.inventorizers:
            logger.info(f"Inventorizing the permissions for objects of type(s) {inventorizer.logical_object_types}")
            inventorizer.preload()
            collected = inventorizer.inventorize()
            if collected:
                self._workspace_inventory.save(collected)
            else:
                logger.warning(f"No objects of type {inventorizer.logical_object_types} were found")

        logger.info("Permissions were inventorized and saved")

    @staticmethod
    def __prepare_request_for_permissions_api(
        item: WorkspacePermissions,
        migration_state: GroupMigrationState,
        destination: Literal["backup", "account"],
    ) -> PermissionRequestPayload:
        _existing_permissions: ObjectPermissions = item.object_permissions
        _acl = _existing_permissions.access_control_list
        acl_requests = []

        for _item in _acl:
            # TODO: we have a double iteration over migration_state.groups
            #  (also by migration_state.get_by_workspace_group_name).
            #  Has to be be fixed by iterating just on .groups
            if _item.group_name in [g.workspace.display_name for g in migration_state.groups]:
                migration_info = migration_state.get_by_workspace_group_name(_item.group_name)
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
        item: WorkspacePermissions,
        migration_state: GroupMigrationState,
        destination: Literal["backup", "account"],
    ) -> SecretsPermissionRequestPayload:
        _final_acls = []

        logger.debug("Preparing the permissions for the secrets API")

        for _existing_acl in item.secret_scope_acls:
            _new_acl = deepcopy(_existing_acl)

            if _existing_acl.principal in [g.workspace.display_name for g in migration_state.groups]:
                migration_info = migration_state.get_by_workspace_group_name(_existing_acl.principal)
                assert (
                    migration_info is not None
                ), f"Group {_existing_acl.principal} is not in the migration groups provider"
                destination_group: Group = getattr(migration_info, destination)
                _new_acl = replace(_existing_acl, principal=destination_group.display_name)
                _final_acls.append(_new_acl)

        return SecretsPermissionRequestPayload(
            object_id=item.object_id,
            access_control_list=_final_acls,
        )

    @staticmethod
    def __prepare_request_for_roles_and_entitlements(
        item: WorkspacePermissions, migration_state: GroupMigrationState, destination
    ) -> RolesAndEntitlementsRequestPayload:
        # TODO: potential BUG - why does item.object_id hold a group name and not ID?
        migration_info = migration_state.get_by_workspace_group_name(item.object_id)
        assert migration_info is not None, f"Group {item.object_id} is not in the migration groups provider"
        destination_group: Group = getattr(migration_info, destination)
        return RolesAndEntitlementsRequestPayload(payload=item.roles_and_entitlements, group_id=destination_group.id)

    def _prepare_new_permission_request(
        self,
        item: WorkspacePermissions,
        migration_state: GroupMigrationState,
        destination: Literal["backup", "account"],
    ) -> AnyRequestPayload:
        # TODO: move this and other relevant methods to WorkspacePermissions
        if isinstance(item.request_object_type, RequestObjectType) and isinstance(
            item.object_permissions, ObjectPermissions
        ):
            return self.__prepare_request_for_permissions_api(item, migration_state, destination)
        elif item.logical_object_type == LogicalObjectType.SECRET_SCOPE:
            return self._prepare_permission_request_for_secrets_api(item, migration_state, destination)
        elif item.logical_object_type in [LogicalObjectType.ROLES, LogicalObjectType.ENTITLEMENTS]:
            return self.__prepare_request_for_roles_and_entitlements(item, migration_state, destination)
        else:
            logger.warning(
                f"Unsupported permissions payload for object {item.object_id} "
                f"with logical type {item.logical_object_type}"
            )

    @retry(wait=wait_fixed(1) + wait_random(0, 2), stop=stop_after_attempt(5))
    def _scope_permissions_applicator(self, request_payload: SecretsPermissionRequestPayload):
        for _acl_item in request_payload.access_control_list:
            # this request will create OR update the ACL for the given principal
            # it means that the access_control_list should only keep records required for update
            self._ws.secrets.put_acl(
                scope=request_payload.object_id, principal=_acl_item.principal, permission=_acl_item.permission
            )
            logger.debug(f"Applied new permissions for scope {request_payload.object_id}: {_acl_item}")
            # in-flight check for the applied permissions
            # the api might be inconsistent, therefore we need to check that the permissions were applied
            for _ in range(3):
                time.sleep(random.random() * 2)
                applied_acls = safe_get_acls(
                    self._ws, scope_name=request_payload.object_id, group_name=_acl_item.principal
                )
                assert applied_acls, f"Failed to apply permissions for {_acl_item.principal}"
                assert applied_acls.permission == _acl_item.permission, (
                    f"Failed to apply permissions for {_acl_item.principal}. "
                    f"Expected: {_acl_item.permission}. Actual: {applied_acls.permission}"
                )

    def _standard_permissions_applicator(self, request_payload: PermissionRequestPayload):
        self._ws.update_permissions(
            request_object_type=request_payload.request_object_type,
            request_object_id=request_payload.object_id,
            access_control_list=request_payload.access_control_list,
        )

    def applicator(self, request_payload: AnyRequestPayload):
        if isinstance(request_payload, RolesAndEntitlementsRequestPayload):
            self._ws.apply_roles_and_entitlements(
                group_id=request_payload.group_id,
                roles=request_payload.payload.roles,
                entitlements=request_payload.payload.entitlements,
            )
        elif isinstance(request_payload, PermissionRequestPayload):
            self._standard_permissions_applicator(request_payload)
        elif isinstance(request_payload, SecretsPermissionRequestPayload):
            self._scope_permissions_applicator(request_payload)
        else:
            logger.warning(f"Unsupported payload type {type(request_payload)}")

    def _apply_permissions_in_parallel(
        self,
        requests: list[AnyRequestPayload],
    ):
        executables = [partial(self.applicator, payload) for payload in requests]
        execution = ThreadedExecution[None](executables)
        execution.run()

    def apply_group_permissions(self, migration_state: GroupMigrationState, destination: Literal["backup", "account"]):
        logger.info(f"Applying the permissions to {destination} groups")
        logger.info(f"Total groups to apply permissions: {len(migration_state.groups)}")

        permissions_on_source = self._workspace_inventory.load_for_groups(
            groups=[g.workspace.display_name for g in migration_state.groups]
        )
        permission_payloads: list[AnyRequestPayload] = [
            self._prepare_new_permission_request(item, migration_state, destination=destination)
            for item in permissions_on_source
        ]
        logger.info(f"Applying {len(permission_payloads)} permissions")

        self._apply_permissions_in_parallel(requests=permission_payloads)
        logger.info(f"All permissions were applied for {destination} groups")
