import dataclasses
import json
import logging
import random
import time
from abc import ABC, abstractmethod
from functools import partial

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service.iam import AccessControlRequest, Group, ObjectPermissions
from databricks.sdk.service.sql import AccessControl as SqlAccessControl
from databricks.sdk.service.sql import GetResponse as SqlPermissions
from databricks.sdk.service.sql import ObjectTypePlural as SqlRequestObjectType
from databricks.sdk.service.workspace import AclItem as SdkAclItem
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_fixed, wait_random

from databricks.labs.ucx.inventory.types import (
    AclItemsContainer,
    Destination,
    PermissionsInventoryItem,
    RequestObjectType,
    RolesAndEntitlements,
)
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.utils import ThreadedExecution

logger = logging.getLogger(__name__)

Executable = partial[None]


class BaseApplicator(ABC):
    def __init__(
            self,
            ws: WorkspaceClient,
            migration_state: GroupMigrationState,
            destination: Destination,
            item: PermissionsInventoryItem,
    ):
        self._ws = ws
        self._item = item
        self._destination = destination
        self._migration_state = migration_state

    # please note the order of decorators here
    # When abstractmethod() is applied in combination with other method descriptors,
    # it should be applied as the innermost decorator.
    # src: https://docs.python.org/3/library/abc.html
    @property
    @abstractmethod
    def func(self) -> Executable:
        """
        This method should return a function that will be executed in the threaded execution.
        :return:
        """


class SecretScopeApplicator(BaseApplicator):
    @property
    def func(self):
        _existing_acl_container: AclItemsContainer = self._item.typed_object_permissions
        _final_acls = []

        logger.debug("Preparing the permissions for the secrets API")

        for _existing_acl in _existing_acl_container.acls:

            if _existing_acl.principal in [g.workspace.display_name for g in self._migration_state.groups]:
                migration_info = self._migration_state.get_by_workspace_group_name(_existing_acl.principal)
                assert (
                        migration_info is not None
                ), f"Group {_existing_acl.principal} is not in the migration groups provider"
                destination_group: Group = getattr(migration_info, self._destination)
                _new_acl = dataclasses.replace(_existing_acl, principal=destination_group.display_name)
                _final_acls.append(_new_acl)

        _typed_acl_container = AclItemsContainer(acls=_final_acls)
        return partial(self._apply, self._item.object_id, _typed_acl_container.to_sdk())

    def _secret_scope_permission(self, scope_name: str, group_name: str) -> workspace.AclPermission | None:
        for acl in self._ws.secrets.list_acls(scope=scope_name):
            if acl.principal == group_name:
                return acl.permission
        return None

    @retry(wait=wait_fixed(1) + wait_random(0, 2), stop=stop_after_attempt(5))
    def _apply(self, object_id: str, acl: list[SdkAclItem]):
        for _acl_item in acl:
            self._ws.secrets.put_acl(scope=object_id, principal=_acl_item.principal, permission=_acl_item.permission)
            logger.debug(f"Applied new permissions for scope {object_id}: {_acl_item}")
            # TODO: add mixin to SDK
            # in-flight check for the applied permissions
            # the api might be inconsistent, therefore we need to check that the permissions were applied
            for _ in range(3):
                time.sleep(random.random() * 2)
                applied_permission = self._secret_scope_permission(scope_name=object_id, group_name=_acl_item.principal)
                assert applied_permission, f"Failed to apply permissions for {_acl_item.principal}"
                assert applied_permission == _acl_item.permission, (
                    f"Failed to apply permissions for {_acl_item.principal}. "
                    f"Expected: {_acl_item.permission}. Actual: {applied_permission}"
                )


class RolesAndEntitlementsApplicator(BaseApplicator):
    @property
    def func(self):
        migration_info = self._migration_state.get_by_workspace_group_name(
            self._item.typed_object_permissions.group_name
        )
        assert migration_info is not None, f"Group {self._item.object_id} is not in the migration groups provider"
        destination_group: Group = getattr(migration_info, self._destination)
        return partial(self._apply, destination_group.id, self._item.typed_object_permissions)

    def _patch_workspace_group(self, group_id: str, payload: dict):
        # TODO: replace usages
        # self.groups.patch(group_id,
        #                   schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
        #                   operations=[
        #                       Patch(op=PatchOp.ADD, path='..', value='...')
        #                   ])
        path = f"/api/2.0/preview/scim/v2/Groups/{group_id}"
        self._ws.api_client.do("PATCH", path, data=json.dumps(payload))

    @sleep_and_retry
    @limits(calls=10, period=1)  # assumption
    def _apply(self, group_id: str, payload: RolesAndEntitlements):
        # TODO: move to other places, this won't be in SDK
        op_schema = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
        schemas = []
        operations = []

        if payload.entitlements:
            schemas.append(op_schema)
            entitlements_payload = {
                "op": "add",
                "path": "entitlements",
                "value": payload.entitlements,
            }
            operations.append(entitlements_payload)

        if payload.roles:
            schemas.append(op_schema)
            roles_payload = {
                "op": "add",
                "path": "roles",
                "value": payload.roles,
            }
            operations.append(roles_payload)

        if operations:
            request = {
                "schemas": schemas,
                "Operations": operations,
            }
            self._patch_workspace_group(group_id, request)


class ObjectPermissionsApplicator(BaseApplicator):
    @property
    def func(self):
        _existing_permissions: ObjectPermissions = self._item.typed_object_permissions
        _acl = _existing_permissions.access_control_list
        acl_requests = []

        for _item in _acl:
            # TODO: we have a double iteration over migration_state.groups
            #  (also by migration_state.get_by_workspace_group_name).
            #  Has to be be fixed by iterating just on .groups
            if _item.group_name in [g.workspace.display_name for g in self._migration_state.groups]:
                migration_info = self._migration_state.get_by_workspace_group_name(_item.group_name)
                assert migration_info is not None, f"Group {_item.group_name} is not in the migration groups provider"
                destination_group: Group = getattr(migration_info, self._destination)
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

        return partial(self._update_permissions, self._item.request_object_type, self._item.object_id, acl_requests)

    @sleep_and_retry
    @limits(calls=30, period=1)
    def _update_permissions(
            self,
            request_object_type: RequestObjectType,
            request_object_id: str,
            access_control_list: list[AccessControlRequest],
    ):
        self._ws.permissions.update(
            request_object_type=request_object_type,
            request_object_id=request_object_id,
            access_control_list=access_control_list,
        )


class SqlPermissionsApplicator(BaseApplicator):
    @property
    def func(self):
        _existing_permissions: SqlPermissions = self._item.typed_object_permissions
        _acl = _existing_permissions.access_control_list
        acl_requests: list[SqlAccessControl] = []

        for acl_request in _acl:
            if acl_request.group_name in [g.workspace.display_name for g in self._migration_state.groups]:
                migration_info = self._migration_state.get_by_workspace_group_name(acl_request.group_name)
                assert (
                        migration_info is not None
                ), f"Group {acl_request.group_name} is not in the migration groups provider"
                destination_group: Group = getattr(migration_info, self._destination)
                acl_request.group_name = destination_group.display_name
                acl_requests.append(acl_request)
            else:
                # no changes shall be applied
                acl_requests.append(acl_request)

        return partial(self._set_permissions, self._item.request_object_type, self._item.object_id, acl_requests)

    @sleep_and_retry
    @limits(calls=30, period=1)
    def _set_permissions(
            self, object_type: SqlRequestObjectType, object_id: str, access_control_list: list[SqlAccessControl]
    ):
        self._ws.dbsql_permissions.set(
            object_id=object_id,
            object_type=object_type,
            access_control_list=access_control_list,
        )


class Applicators:
    """
    Main entrypoint for the applicators.
    Please don't use the applicators directly, use this class instead.
    However, for single-object testing you can directly use specific applicators.
    """

    def __init__(self, ws: WorkspaceClient, migration_state: GroupMigrationState, destination: Destination):
        self._ws = ws
        self._migration_state = migration_state
        self._destination = destination
        self._executables: list[Executable] = []

    def _get_applicator(self, item: PermissionsInventoryItem) -> BaseApplicator:
        typed_acl_payload = item.typed_object_permissions
        if isinstance(typed_acl_payload, ObjectPermissions):
            return ObjectPermissionsApplicator(self._ws, self._migration_state, self._destination, item)
        elif isinstance(typed_acl_payload, SqlPermissions):
            return SqlPermissionsApplicator(self._ws, self._migration_state, self._destination, item)
        elif isinstance(typed_acl_payload, AclItemsContainer):
            return SecretScopeApplicator(self._ws, self._migration_state, self._destination, item)
        elif isinstance(typed_acl_payload, RolesAndEntitlements):
            return RolesAndEntitlementsApplicator(self._ws, self._migration_state, self._destination, item)
        else:
            msg = f"Unknown type {type(typed_acl_payload)}"
            raise NotImplementedError(msg)

    def prepare(self, items: list[PermissionsInventoryItem]):
        """
        This method should return the correct applicator for the given item.
        """
        self._executables = [self._get_applicator(item).func for item in items]

    def apply(self):
        """
        This method should apply the changes.
        """
        execution = ThreadedExecution(self._executables)
        execution.run()
