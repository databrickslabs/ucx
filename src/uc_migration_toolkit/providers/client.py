import json
from collections.abc import Iterator
from dataclasses import asdict

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, Group
from databricks.sdk.service.workspace import ObjectType
from ratelimit import limits, sleep_and_retry
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from uc_migration_toolkit.config import AuthConfig
from uc_migration_toolkit.managers.inventory.types import RequestObjectType
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger


class ImprovedWorkspaceClient(WorkspaceClient):
    # to this class we add rate-limited methods to make calls to various APIs
    # source info - https://docs.databricks.com/resources/limits.html

    @sleep_and_retry
    @limits(calls=5, period=1)  # assumption
    def assign_permissions(self, principal_id: str, permissions: list[str]):
        request_string = f"/api/2.0/preview/permissionassignments/principals/{principal_id}"
        self.api_client.do("put", request_string, data=json.dumps({"permissions": permissions}))

    @sleep_and_retry
    @limits(calls=10, period=1)  # assumption
    def patch_workspace_group(self, group_id: str, payload: dict):
        path = f"/api/2.0/preview/scim/v2/Groups/{group_id}"
        self.api_client.do("PATCH", path, data=json.dumps(payload))

    @sleep_and_retry
    @limits(calls=100, period=1)  # assumption
    def list_account_level_groups(
        self, filter: str, attributes: str | None = None, excluded_attributes: str | None = None  # noqa: A002
    ) -> list[Group]:
        query = {"filter": filter, "attributes": attributes, "excludedAttributes": excluded_attributes}
        response = self.api_client.do("get", "/api/2.0/account/scim/v2/Groups", query=query)
        return [Group.from_dict(v) for v in response.get("Resources", [])]

    def reflect_account_group_to_workspace(self, acc_group: Group) -> None:
        logger.info(f"Reflecting group {acc_group.display_name} to workspace")
        self.assign_permissions(principal_id=acc_group.id, permissions=["USER"])
        logger.info(f"Group {acc_group.display_name} successfully reflected to workspace")

    @sleep_and_retry
    @limits(calls=100, period=1)  # assumption
    def get_tokens(self):
        return self.api_client.do("GET", "/api/2.0/preview/permissions/authorization/tokens")

    @sleep_and_retry
    @limits(calls=100, period=1)  # assumption
    def get_passwords(self):
        return self.api_client.do("GET", "/api/2.0/preview/permissions/authorization/passwords")

    @sleep_and_retry
    @limits(calls=45, period=1)  # safety value, can be 50 actually
    def list_workspace(self, path: str) -> Iterator[ObjectType]:
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


class ClientProvider:
    def __init__(self):
        self._ws_client: ImprovedWorkspaceClient | None = None

    @staticmethod
    def _verify_ws_client(w: ImprovedWorkspaceClient):
        assert w.current_user.me(), "Cannot authenticate with the workspace client"
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    @staticmethod
    def __get_retry_strategy():
        retry_strategy = Retry(
            total=20,
            backoff_factor=1,
            status_forcelist=[429],
            respect_retry_after_header=True,
            raise_on_status=False,  # return original response when retries have been exhausted
            # adjusted from the default values
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "PATCH", "POST"],
        )
        return retry_strategy

    def _adjust_session(self, client: ImprovedWorkspaceClient, pool_size: int | None = None):
        pool_size = pool_size if pool_size else config_provider.config.num_threads
        logger.debug(f"Adjusting the session to fully utilize {pool_size} threads")
        _existing_session = client.api_client._session
        _session = requests.Session()
        _session.auth = _existing_session.auth
        _session.mount("https://", HTTPAdapter(max_retries=self.__get_retry_strategy(), pool_maxsize=pool_size))
        client.api_client._session = _session
        logger.debug("Session adjusted")

    def set_ws_client(self, auth_config: AuthConfig | None = None, pool_size: int | None = None):
        if self._ws_client:
            logger.warning("Workspace client already initialized, skipping")
            return

        logger.info("Initializing the workspace client")
        if auth_config and auth_config.workspace:
            logger.info("Using the provided workspace client credentials")
            _client = ImprovedWorkspaceClient(**asdict(auth_config.workspace))
        else:
            logger.info("Trying standard workspace auth mechanisms")
            _client = ImprovedWorkspaceClient()

        self._verify_ws_client(_client)
        self._adjust_session(_client, pool_size)
        self._ws_client = _client

    @property
    def ws(self) -> ImprovedWorkspaceClient:
        assert self._ws_client, "Workspace client not initialized"
        return self._ws_client


provider = ClientProvider()
