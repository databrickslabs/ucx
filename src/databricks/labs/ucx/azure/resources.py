import re
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property

from databricks.sdk.core import (
    ApiClient,
    AzureCliTokenSource,
    Config,
    credentials_provider,
)
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceConflict

from databricks.labs.ucx.assessment.crawlers import logger

# https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles
_ROLES = {"STORAGE_BLOB_DATA_READER": "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1"}


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
    # service principal will have type: "Application"
    # managed identity will have type: "ManagedIdentity"
    type: str
    # Need this directory_id/tenant_id when create UC storage credentials using service principal
    # it will be None if type is managed identity
    directory_id: str | None = None


@dataclass
class PrincipalSecret:
    client: Principal
    secret: str


@dataclass
class AzureRoleAssignment:
    resource: AzureResource
    scope: AzureResource
    principal: Principal
    role_name: str


@dataclass
class AccessConnector:
    id: str
    name: str
    type: str
    location: str
    # TODO: Add identity with reference to dataclass
    # identity {"principalId": ..., "tenantId": ..., "type": ...}
    # The raw API call contains the following fields as well:
    # tags
    # properties
    # systemData

    _pattern_id = re.compile(
        r"^/subscriptions/([\w-]+)/resourceGroups/([\w-]+)"
        r"/providers/Microsoft\.Databricks/accessConnectors/([\w-]+)$"
    )

    def _parse_id(self) -> tuple[str, str]:
        match = self._pattern_id.match(self.id)
        assert match is not None
        subscription_id, resource_group, _ = match.groups()
        return subscription_id, resource_group

    @property
    def subscription_id(self) -> str:
        subscription_id, _ = self._parse_id()
        return subscription_id

    @property
    def resource_group(self) -> str:
        _, resource_group = self._parse_id()
        return resource_group


class AzureAPIClient:
    def __init__(self, host_endpoint: str, service_endpoint: str):
        self.api_client = ApiClient(
            Config(
                host=host_endpoint,
                credentials_provider=self._provider_for(service_endpoint),
            )
        )
        self._token_source = AzureCliTokenSource(host_endpoint)

    def _provider_for(self, endpoint: str):
        @credentials_provider("azure-cli", ["host"])
        def _credentials(_: Config):
            token_source = AzureCliTokenSource(endpoint)

            def inner() -> dict[str, str]:
                token = token_source.token()
                return {"Authorization": f"{token.token_type} {token.access_token}"}

            return inner

        return _credentials

    def get(self, path: str, api_version: str | None = None):
        headers = {"Accept": "application/json"}
        query = {}
        if api_version is not None:
            query = {"api-version": api_version}
        return self.api_client.do("GET", path, query, headers)

    def put(self, path: str, api_version: str | None = None, body: dict | None = None):
        headers = {"Content-Type": "application/json"}
        query: dict[str, str] = {}
        if api_version is not None:
            query = {"api-version": api_version}
        if body is not None:
            return self.api_client.do("PUT", path, query, headers, body)
        return None

    def post(self, path: str, body: dict | None = None):
        headers = {"Content-Type": "application/json"}
        query: dict[str, str] = {}
        if body is not None:
            return self.api_client.do("POST", path, query, headers, body)
        return self.api_client.do("POST", path, query, headers)

    def delete(self, path: str, api_version: str | None = None):
        # this method is added only to be used in int test to delete the application once tests pass
        headers = {"Content-Type": "application/json"}
        query: dict[str, str] = {}
        if api_version is not None:
            query = {"api-version": api_version}
        return self.api_client.do("DELETE", path, query, headers)

    def token(self):
        return self._token_source.token()


class AccessConnectorClient:

    def __init__(self, azure_mgmt: AzureAPIClient) -> None:
        self._api_version = "2023-05-01"
        self._azure_mgmt = azure_mgmt

    def list(self, subscription_id: str) -> list[AccessConnector]:
        """List all access connector within subscription
        
        Docs:
            https://learn.microsoft.com/en-us/rest/api/databricks/access-connectors/list-by-subscription?view=rest-databricks-2023-05-01&tabs=HTTP    
        """
        url = f"/subscriptions/{subscription_id}/providers/Microsoft.Databricks/accessConnectors"
        response = self._azure_mgmt.get(url, self._api_version)

        access_connectors = []
        for access_connector_raw in response["value"]:
            access_connector = AccessConnector(
                id=access_connector_raw["id"],
                name=access_connector_raw["name"],
                type=access_connector_raw["type"],
                location=access_connector_raw["location"],
            )
            access_connectors.append(access_connector)
        return access_connectors

    def create(self, access_connector: AccessConnector):
        """Create access connector.

        Docs:
            https://learn.microsoft.com/en-us/rest/api/databricks/access-connectors/create-or-update?view=rest-databricks-2023-05-01&tabs=HTTP
        """
        # TODO: Manual test failed, fix!
        body = {
            "location": access_connector.location
        }
        self._azure_mgmt.put(access_connector.id, self._api_version, body)

    def delete(self, access_connector: AccessConnector):
        """Delete an access connector.

        Docs:
            https://learn.microsoft.com/en-us/rest/api/databricks/access-connectors/delete?view=rest-databricks-2023-05-01&tabs=HTTP
        """
        self._azure_mgmt.delete(access_connector.id)


class AzureResources:
    def __init__(self, azure_mgmt: AzureAPIClient, azure_graph: AzureAPIClient, include_subscriptions=None):
        if not include_subscriptions:
            include_subscriptions = []
        self._mgmt = azure_mgmt
        self._graph = azure_graph
        self._include_subscriptions = include_subscriptions
        self._role_definitions = {}  # type: dict[str, str]
        self._principals: dict[str, Principal | None] = {}

    def _get_subscriptions(self) -> Iterable[AzureSubscription]:
        for subscription in self._mgmt.get("/subscriptions", "2022-12-01").get("value", []):
            yield AzureSubscription(
                name=subscription["displayName"],
                subscription_id=subscription["subscriptionId"],
                tenant_id=subscription["tenantId"],
            )

    def create_service_principal(self, display_name: str) -> PrincipalSecret:
        try:
            application_info: dict[str, str] = self._graph.post("/v1.0/applications", {"displayName": display_name})
            app_id = application_info.get("appId")
            assert app_id is not None
            service_principal_info: dict[str, str] = self._graph.post("/v1.0/servicePrincipals", {"appId": app_id})
            object_id = service_principal_info.get("id")
            assert object_id is not None
            secret_info: dict[str, str] = self._graph.post(f"/v1.0/servicePrincipals/{object_id}/addPassword")

        except PermissionDenied:
            msg = (
                "Permission denied. Please run this cmd under the identity of a user who has"
                " create service principal permission."
            )
            logger.error(msg)
            raise PermissionDenied(msg) from None
        secret = secret_info.get("secretText")
        client_id = service_principal_info.get("appId")
        principal_type = service_principal_info.get("servicePrincipalType")
        directory_id = service_principal_info.get("appOwnerOrganizationId")
        assert client_id is not None
        assert object_id is not None
        assert principal_type is not None
        assert directory_id is not None
        assert secret is not None
        return PrincipalSecret(Principal(client_id, display_name, object_id, principal_type, directory_id), secret)

    def delete_service_principal(self, principal_id: str):
        try:
            self._graph.delete(f"/v1.0/applications(appId='{principal_id}')")
        except PermissionDenied:
            msg = f"User doesnt have permission to delete application {principal_id}"
            logger.error(msg)
            raise PermissionDenied(msg) from None

    def apply_storage_permission(self, principal_id: str, resource: AzureResource, role_name: str, role_guid: str):
        try:
            role_id = _ROLES[role_name]
            path = f"{str(resource)}/providers/Microsoft.Authorization/roleAssignments/{role_guid}"
            role_definition_id = (
                f"/subscriptions/{resource.subscription_id}/providers/Microsoft.Authorization/roleDefinitions/{role_id}"
            )
            body = {
                "properties": {
                    "roleDefinitionId": role_definition_id,
                    "principalId": principal_id,
                    "principalType": "ServicePrincipal",
                }
            }
            self._mgmt.put(path, "2022-04-01", body)
        except ResourceConflict:
            logger.warning(
                f"Role assignment already exists for role {role_guid} on storage {resource.storage_account}"
                f" for spn {principal_id}."
            )
        except PermissionDenied:
            msg = (
                "Permission denied. Please run this cmd under the identity of a user who has "
                "create service principal permission."
            )
            logger.error(msg)
            raise PermissionDenied(msg) from None

    def tenant_id(self):
        token = self._mgmt.token()
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
            for storage in self._mgmt.get(path, "2023-01-01").get("value", []):
                resource_id = storage.get("id")
                if not resource_id:
                    continue
                yield AzureResource(resource_id)

    def containers(self, storage: AzureResource):
        for raw in self._mgmt.get(f"{storage}/blobServices/default/containers", "2023-01-01").get("value", []):
            resource_id = raw.get("id")
            if not resource_id:
                continue
            yield AzureResource(resource_id)

    def _get_principal(self, principal_id: str) -> Principal | None:
        if principal_id in self._principals:
            return self._principals[principal_id]
        try:
            path = f"/v1.0/directoryObjects/{principal_id}"
            raw: dict[str, str] = self._graph.get(path)  # type: ignore[assignment]
        except NotFound:
            # don't load principals from external directories twice
            self._principals[principal_id] = None
            return self._principals[principal_id]

        client_id = raw.get("appId")
        display_name = raw.get("displayName")
        object_id = raw.get("id")
        principal_type = raw.get("servicePrincipalType")
        # Need this directory_id/tenant_id when create UC storage credentials using service principal
        directory_id = raw.get("appOwnerOrganizationId")
        assert client_id is not None
        assert display_name is not None
        assert object_id is not None
        assert principal_type is not None
        if principal_type == "Application":
            # service principal must have directory_id
            assert directory_id is not None
        self._principals[principal_id] = Principal(client_id, display_name, object_id, principal_type, directory_id)
        return self._principals[principal_id]

    def role_assignments(
        self, resource_id: str, *, principal_types: list[str] | None = None
    ) -> Iterable[AzureRoleAssignment]:
        """See https://learn.microsoft.com/en-us/rest/api/authorization/role-assignments/list-for-resource"""
        if not principal_types:
            principal_types = ["ServicePrincipal"]
        result = self._mgmt.get(f"{resource_id}/providers/Microsoft.Authorization/roleAssignments", "2022-04-01")
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
            role_definition = self._mgmt.get(role_definition_id, "2022-04-01")
            definition_properties = role_definition.get("properties", {})
            role_name: str = definition_properties.get("roleName")
            if not role_name:
                return None
            self._role_definitions[role_definition_id] = role_name
        return self._role_definitions.get(role_definition_id)

    def managed_identity_client_id(
        self, access_connector_id: str, user_assigned_identity_id: str | None = None
    ) -> str | None:
        # get te client_id/application_id of the managed identity used in the access connector
        try:
            identity = self._mgmt.get(access_connector_id, "2023-05-01").get("identity")
        except NotFound:
            logger.warning(f"Access connector {access_connector_id} no longer exists")
            return None
        if not identity:
            return None

        if identity.get("type") == "UserAssigned":
            if not user_assigned_identity_id:
                return None
            identities = identity.get("userAssignedIdentities")
            if user_assigned_identity_id in identities:
                return identities.get(user_assigned_identity_id).get("clientId")
            # sometimes we see "resourceGroups" instead of "resourcegroups" in the response from Azure RM API
            # but "resourcegroups" is in response from storage credential's managed_identity_id
            alternative_identity_id = user_assigned_identity_id.replace("resourcegroups", "resourceGroups")
            if alternative_identity_id in identities:
                return identities.get(alternative_identity_id).get("clientId")
            return None
        if identity.get("type") == "SystemAssigned":
            # SystemAssigned managed identity does not have client_id in get access connector response
            # need to call graph api directoryObjects to fetch the client_id
            principal = self._get_principal(identity.get("principalId"))
            if not principal:
                return None
            return principal.client_id
        return None

    @cached_property
    def access_connector_handler(self) -> AccessConnectorClient:
        return AccessConnectorClient(self._mgmt)