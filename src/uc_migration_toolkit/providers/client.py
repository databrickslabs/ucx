import json
from dataclasses import asdict

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import Group
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from uc_migration_toolkit.config import AuthConfig
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger


class ImprovedWorkspaceClient(WorkspaceClient):
    def assign_permissions(self, principal_id: str, permissions: list[str]):
        request_string = f"/api/2.0/preview/permissionassignments/principals/{principal_id}"

        self.api_client.do("put", request_string, data=json.dumps({"permissions": permissions}))

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
        # Since urllib3 v1.26.0, Retry.DEFAULT_METHOD_WHITELIST is deprecated in favor of
        # Retry.DEFAULT_ALLOWED_METHODS. We need to support both versions.
        if "DEFAULT_ALLOWED_METHODS" in dir(Retry):
            retry_kwargs = {"allowed_methods": {"POST"} | set(Retry.DEFAULT_ALLOWED_METHODS)}
        else:
            retry_kwargs = {'method_whitelist': {"POST"} | set(Retry.DEFAULT_METHOD_WHITELIST)}  # noqa

        retry_strategy = Retry(
            total=6,
            backoff_factor=1,
            status_forcelist=[429],
            respect_retry_after_header=True,
            raise_on_status=False,  # return original response when retries have been exhausted
            **retry_kwargs,
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
