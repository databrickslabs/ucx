from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import (
    ApiClient,
    AzureCliTokenSource,
    Config,
    credentials_provider,
)
from databricks.sdk.errors import BadRequest, NotFound, PermissionDenied

from databricks.labs.ucx.assessment.crawlers import logger

# https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles
_ROLES = {
    "STORAGE_BLOB_READER": {
        "name": "e97fa67e-cf3a-49f4-987b-2fc8a3be88a1",
        "id": "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1",
    }
}


@dataclass
class AzureSubscription:
    name: str
    subscription_id: str
    tenant_id: str


class AzureResource:
    def __init__(self, resource_id: str):
        self._pairs = {}
        self._resource_id = resource_id
        split = resource_id.lstrip("/").split("/")
        if len(split) % 2 != 0:
            msg = f"not a list of pairs: {resource_id}"
            raise ValueError(msg)
        i = 0
        while i < len(split):
            k = split[i]
            value = split[i + 1]
            i += 2
            self._pairs[k] = value

    @property
    def subscription_id(self):
        return self._pairs.get("subscriptions")

    @property
    def resource_group(self):
        return self._pairs.get("resourceGroups")

    @property
    def storage_account(self):
        return self._pairs.get("storageAccounts")

    @property
    def container(self):
        return self._pairs.get("containers")

    def __eq__(self, other):
        if not isinstance(other, AzureResource):
            return NotImplemented
        return self._resource_id == other._resource_id

    def __repr__(self):
        properties = ["subscription_id", "resource_group", "storage_account", "container"]
        pairs = [f"{_}={getattr(self, _)}" for _ in properties]
        return f'AzureResource<{", ".join(pairs)}>'

    def __str__(self):
        return self._resource_id


@dataclass
class Principal:
    client_id: str
    display_name: str
    object_id: str
    # Need this directory_id/tenant_id when create UC storage credentials using service principal
    directory_id: str
    secret: str | None = None


@dataclass
class AzureRoleAssignment:
    resource: AzureResource
    scope: AzureResource
    principal: Principal
    role_name: str


class AzureAPIClient:
    def __init__(self, ws: WorkspaceClient):
        rm_host = ws.config.arm_environment.resource_manager_endpoint
        self._resource_manager = ApiClient(
            Config(
                host=rm_host,
                credentials_provider=self._provider_for(ws.config.arm_environment.service_management_endpoint),
            )
        )
        self._graph = ApiClient(
            Config(
                host="https://graph.microsoft.com",
                credentials_provider=self._provider_for("https://graph.microsoft.com"),
            )
        )
        self._token_source = AzureCliTokenSource(rm_host)

    def _provider_for(self, endpoint: str):
        @credentials_provider("azure-cli", ["host"])
        def _credentials(_: Config):
            token_source = AzureCliTokenSource(endpoint)

            def inner() -> dict[str, str]:
                token = token_source.token()
                return {"Authorization": f"{token.token_type} {token.access_token}"}

            return inner

        return _credentials

    def get(self, path: str, host: str, api_version: str | None = None):
        headers = {"Accept": "application/json"}
        query = {}
        if api_version is not None:
            query = {"api-version": api_version}
        if host == "azure_mgmt":
            return self._resource_manager.do("GET", path, query, headers)
        if host == "azure_graph":
            return self._graph.do("GET", path)
        return None

    def put(self, path: str, host: str, body: dict | None = None):
        headers = {"Content-Type": "application/json"}
        if host == "azure_mgmt":
            assert body is not None
            query = {"api-version": "2022-04-01"}
            return self._resource_manager.do("PUT", path, headers=headers, body=body, query=query)
        if host == "azure_graph":
            if body is not None:
                return self._graph.do("POST", path, headers=headers, body=body)
            return self._graph.do("POST", path, headers=headers)
        raise ValueError()

    @property
    def token(self):
        return self._token_source.token()


class AzureResources:
    def __init__(self, *, include_subscriptions=None, api_client: AzureAPIClient):
        if not include_subscriptions:
            include_subscriptions = []
        self._api_client = api_client
        self._include_subscriptions = include_subscriptions
        self._role_definitions = {}  # type: dict[str, str]
        self._principals: dict[str, Principal | None] = {}

    def _get_subscriptions(self) -> Iterable[AzureSubscription]:
        for subscription in self._api_client.get("/subscriptions", "azure_mgmt", "2022-12-01").get("value", []):
            yield AzureSubscription(
                name=subscription["displayName"],
                subscription_id=subscription["subscriptionId"],
                tenant_id=subscription["tenantId"],
            )

    def create_service_principal(self) -> Principal:
        try:
            application_info: dict[str, str] = self._api_client.put(
                "/v1.0/applications", "azure_graph", {"displayName": "UCXServicePrincipal"}
            )
            app_id = application_info.get("appId")
            assert app_id is not None
            service_principal_info: dict[str, str] = self._api_client.put(
                "/v1.0/servicePrincipals", "azure_graph", {"appId": app_id}
            )
            client_id = service_principal_info.get("appId")
            assert client_id is not None
            secret_info: dict[str, str] = self._api_client.put(
                f"/v1.0/servicePrincipals(appId='{client_id}')/addPassword", "azure_graph"
            )

        except PermissionDenied:
            msg = (
                "Permission denied. Please run this cmd under the identity of a user who has"
                " create service principal permission."
            )
            logger.error(msg)
            raise PermissionDenied(msg) from None
        secret = secret_info.get("secretText")
        display_name = service_principal_info.get("displayName")
        object_id = service_principal_info.get("id")
        directory_id = service_principal_info.get("appOwnerOrganizationId")
        assert client_id is not None
        assert display_name is not None
        assert object_id is not None
        assert directory_id is not None
        assert secret is not None
        return Principal(client_id, display_name, object_id, directory_id, secret)

    def apply_storage_permission(self, principal_id: str, resource: AzureResource, role: str):
        try:
            role_name = _ROLES[role]["name"]
            role_id = _ROLES[role]["id"]
            path = f"{str(resource)}/providers/Microsoft.Authorization/roleAssignments/{role_name}"
            body = {
                "properties": {
                    "roleDefinitionId": f"/subscriptions/{resource.subscription_id}/providers/Microsoft.Authorization/roleDefinitions/"
                    f"{role_id}",
                    "principalId": principal_id,
                    "principalType": "ServicePrincipal",
                }
            }
            self._api_client.put(path, "azure_mgmt", body)
        except PermissionDenied:
            msg = (
                "Permission denied. Please run this cmd under the identity of a user who has "
                "create service principal permission."
            )
            logger.error(msg)
            raise PermissionDenied(msg) from None
        except BadRequest:
            pass

    def tenant_id(self):
        token = self._api_client.token
        return token.jwt_claims().get("tid")

    def subscriptions(self):
        tenant_id = self.tenant_id()
        for subscription in self._get_subscriptions():
            if subscription.tenant_id != tenant_id:
                continue
            if subscription.subscription_id not in self._include_subscriptions:
                continue
            yield subscription

    def storage_accounts(self) -> Iterable[AzureResource]:
        for subscription in self.subscriptions():
            logger.info(f"Checking in subscription {subscription.name} for storage accounts")
            path = f"/subscriptions/{subscription.subscription_id}/providers/Microsoft.Storage/storageAccounts"
            for storage in self._api_client.get(path, "azure_mgmt", "2023-01-01").get("value", []):
                resource_id = storage.get("id")
                if not resource_id:
                    continue
                yield AzureResource(resource_id)

    def containers(self, storage: AzureResource):
        for raw in self._api_client.get(f"{storage}/blobServices/default/containers", "azure_mgmt", "2023-01-01").get(
            "value", []
        ):
            resource_id = raw.get("id")
            if not resource_id:
                continue
            yield AzureResource(resource_id)

    def _get_principal(self, principal_id: str) -> Principal | None:
        if principal_id in self._principals:
            return self._principals[principal_id]
        try:
            path = f"/v1.0/directoryObjects/{principal_id}"
            raw: dict[str, str] = self._api_client.get(path, "azure_graph")  # type: ignore[assignment]
        except NotFound:
            # don't load principals from external directories twice
            self._principals[principal_id] = None
            return self._principals[principal_id]

        client_id = raw.get("appId")
        display_name = raw.get("displayName")
        object_id = raw.get("id")
        # Need this directory_id/tenant_id when create UC storage credentials using service principal
        directory_id = raw.get("appOwnerOrganizationId")
        assert client_id is not None
        assert display_name is not None
        assert object_id is not None
        assert directory_id is not None
        self._principals[principal_id] = Principal(client_id, display_name, object_id, directory_id)
        return self._principals[principal_id]

    def role_assignments(
        self, resource_id: str, *, principal_types: list[str] | None = None
    ) -> Iterable[AzureRoleAssignment]:
        """See https://learn.microsoft.com/en-us/rest/api/authorization/role-assignments/list-for-resource"""
        if not principal_types:
            principal_types = ["ServicePrincipal"]
        result = self._api_client.get(
            f"{resource_id}/providers/Microsoft.Authorization/roleAssignments", "azure_mgmt", "2022-04-01"
        )
        for role_assignment in result.get("value", []):
            assignment = self._role_assignment(role_assignment, resource_id, principal_types)
            if not assignment:
                continue
            yield assignment

    def _role_assignment(
        self, role_assignment: dict, resource_id: str, principal_types: list[str]
    ) -> AzureRoleAssignment | None:
        assignment_properties = role_assignment.get("properties", {})
        principal_type = assignment_properties.get("principalType")
        if not principal_type:
            return None
        if principal_type not in principal_types:
            return None
        principal_id = assignment_properties.get("principalId")
        if not principal_id:
            return None
        role_definition_id = assignment_properties.get("roleDefinitionId")
        if not role_definition_id:
            return None
        scope = assignment_properties.get("scope")
        if not scope:
            return None
        role_name = self._role_name(role_definition_id)
        if not role_name:
            return None
        principal = self._get_principal(principal_id)
        if not principal:
            return None
        if scope == "/":
            scope = resource_id
        return AzureRoleAssignment(
            resource=AzureResource(resource_id), scope=AzureResource(scope), principal=principal, role_name=role_name
        )

    def _role_name(self, role_definition_id) -> str | None:
        if role_definition_id not in self._role_definitions:
            role_definition = self._api_client.get(role_definition_id, "azure_mgmt", "2022-04-01")
            definition_properties = role_definition.get("properties", {})
            role_name: str = definition_properties.get("roleName")
            if not role_name:
                return None
            self._role_definitions[role_definition_id] = role_name
        return self._role_definitions.get(role_definition_id)
