from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import (
    ApiClient,
    AzureCliTokenSource,
    Config,
    credentials_provider,
)
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.assessment.crawlers import logger


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
            v = split[i + 1]
            i += 2
            self._pairs[k] = v

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


@dataclass
class AzureRoleAssignment:
    resource: AzureResource
    scope: AzureResource
    principal: Principal
    role_name: str


class AzureResources:
    def __init__(self, ws: WorkspaceClient, *, include_subscriptions=None):
        if not include_subscriptions:
            include_subscriptions = []
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
        self._include_subscriptions = include_subscriptions
        self._role_definitions = {}  # type: dict[str, str]
        self._principals: dict[str, Principal | None] = {}

    def _provider_for(self, endpoint: str):
        @credentials_provider("azure-cli", ["host"])
        def _credentials(_: Config):
            token_source = AzureCliTokenSource(endpoint)

            def inner() -> dict[str, str]:
                token = token_source.token()
                return {"Authorization": f"{token.token_type} {token.access_token}"}

            return inner

        return _credentials

    def _get_subscriptions(self) -> Iterable[AzureSubscription]:
        for subscription in self._get_resource("/subscriptions", api_version="2022-12-01").get("value", []):
            yield AzureSubscription(
                name=subscription["displayName"],
                subscription_id=subscription["subscriptionId"],
                tenant_id=subscription["tenantId"],
            )

    def _tenant_id(self):
        token = self._token_source.token()
        return token.jwt_claims().get("tid")

    def subscriptions(self):
        tenant_id = self._tenant_id()
        for subscription in self._get_subscriptions():
            if subscription.tenant_id != tenant_id:
                continue
            if subscription.subscription_id not in self._include_subscriptions:
                continue
            yield subscription

    def _get_resource(self, path: str, api_version: str):
        headers = {"Accept": "application/json"}
        query = {"api-version": api_version}
        return self._resource_manager.do("GET", path, query=query, headers=headers)

    def storage_accounts(self) -> Iterable[AzureResource]:
        for subscription in self.subscriptions():
            logger.info(f"Checking in subscription {subscription.name} for storage accounts")
            path = f"/subscriptions/{subscription.subscription_id}/providers/Microsoft.Storage/storageAccounts"
            for storage in self._get_resource(path, "2023-01-01").get("value", []):
                resource_id = storage.get("id")
                if not resource_id:
                    continue
                yield AzureResource(resource_id)

    def containers(self, storage: AzureResource):
        for raw in self._get_resource(f"{storage}/blobServices/default/containers", "2023-01-01").get("value", []):
            resource_id = raw.get("id")
            if not resource_id:
                continue
            yield AzureResource(resource_id)

    def _get_principal(self, principal_id: str) -> Principal | None:
        if principal_id in self._principals:
            return self._principals[principal_id]
        try:
            path = f"/v1.0/directoryObjects/{principal_id}"
            raw: dict[str, str] = self._graph.do("GET", path)  # type: ignore[assignment]
        except NotFound:
            # don't load principals from external directories twice
            self._principals[principal_id] = None
            return self._principals[principal_id]
        client_id = raw.get("appId")
        display_name = raw.get("displayName")
        object_id = raw.get("id")
        assert client_id is not None
        assert display_name is not None
        assert object_id is not None
        self._principals[principal_id] = Principal(client_id, display_name, object_id)
        return self._principals[principal_id]

    def role_assignments(
        self, resource_id: str, *, principal_types: list[str] | None = None
    ) -> Iterable[AzureRoleAssignment]:
        """See https://learn.microsoft.com/en-us/rest/api/authorization/role-assignments/list-for-resource"""
        if not principal_types:
            principal_types = ["ServicePrincipal"]
        result = self._get_resource(f"{resource_id}/providers/Microsoft.Authorization/roleAssignments", "2022-04-01")
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
            role_definition = self._get_resource(role_definition_id, "2022-04-01")
            definition_properties = role_definition.get("properties", {})
            role_name: str = definition_properties.get("roleName")
            if not role_name:
                return None
            self._role_definitions[role_definition_id] = role_name
        return self._role_definitions.get(role_definition_id)
