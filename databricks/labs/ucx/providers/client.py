import json
from collections.abc import Iterator

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, Group
from databricks.sdk.service.workspace import ObjectType
from ratelimit import limits, sleep_and_retry

from databricks.labs.ucx.managers.inventory.types import RequestObjectType
from databricks.labs.ucx.providers.logger import logger


class ImprovedWorkspaceClient(WorkspaceClient):
    # ***
    # *** CAUTION: DO NOT ADD ANY METHODS THAT WON'T END UP IN THE SDK ***
    # ***
    # to this class we add rate-limited methods to make calls to various APIs
    # source info - https://docs.databricks.com/resources/limits.html

    @sleep_and_retry
    @limits(calls=5, period=1)  # assumption
    def assign_permissions(self, principal_id: str, permissions: list[str]):
        # TODO: add OpenAPI spec for it
        request_string = f"/api/2.0/preview/permissionassignments/principals/{principal_id}"
        self.api_client.do("put", request_string, data=json.dumps({"permissions": permissions}))

    @sleep_and_retry
    @limits(calls=10, period=1)  # assumption
    def patch_workspace_group(self, group_id: str, payload: dict):
        # TODO: replace usages
        # self.groups.patch(group_id,
        #                   schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
        #                   operations=[
        #                       Patch(op=PatchOp.ADD, path='..', value='...')
        #                   ])
        path = f"/api/2.0/preview/scim/v2/Groups/{group_id}"
        self.api_client.do("PATCH", path, data=json.dumps(payload))

    @sleep_and_retry
    @limits(calls=100, period=1)  # assumption
    def list_account_level_groups(
        self, filter: str, attributes: str | None = None, excluded_attributes: str | None = None  # noqa: A002
    ) -> list[Group]:
        # TODO: move to other places, this won't be in SDK
        query = {"filter": filter, "attributes": attributes, "excludedAttributes": excluded_attributes}
        response = self.api_client.do("get", "/api/2.0/account/scim/v2/Groups", query=query)
        return [Group.from_dict(v) for v in response.get("Resources", [])]

    def reflect_account_group_to_workspace(self, acc_group: Group) -> None:
        logger.info(f"Reflecting group {acc_group.display_name} to workspace")
        self.assign_permissions(principal_id=acc_group.id, permissions=["USER"])
        logger.info(f"Group {acc_group.display_name} successfully reflected to workspace")

    @sleep_and_retry
    @limits(calls=45, period=1)  # safety value, can be 50 actually
    def list_workspace(self, path: str) -> Iterator[ObjectType]:
        # TODO: remove, use SDK
        return self.workspace.list(path=path, recursive=False)

    @sleep_and_retry
    @limits(calls=100, period=1)
    def get_permissions(self, request_object_type: RequestObjectType, request_object_id: str):
        return self.permissions.get(request_object_type=request_object_type, request_object_id=request_object_id)

    @sleep_and_retry
    @limits(calls=30, period=1)
    def update_permissions(
        self,
        request_object_type: RequestObjectType,
        request_object_id: str,
        access_control_list: list[AccessControlRequest],
    ):
        return self.permissions.update(
            request_object_type=request_object_type,
            request_object_id=request_object_id,
            access_control_list=access_control_list,
        )

    def apply_roles_and_entitlements(self, group_id: str, roles: list, entitlements: list):
        # TODO: move to other places, this won't be in SDK
        op_schema = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
        schemas = []
        operations = []

        if entitlements:
            schemas.append(op_schema)
            entitlements_payload = {
                "op": "add",
                "path": "entitlements",
                "value": entitlements,
            }
            operations.append(entitlements_payload)

        if roles:
            schemas.append(op_schema)
            roles_payload = {
                "op": "add",
                "path": "roles",
                "value": roles,
            }
            operations.append(roles_payload)

        if operations:
            request = {
                "schemas": schemas,
                "Operations": operations,
            }
            self.patch_workspace_group(group_id, request)
