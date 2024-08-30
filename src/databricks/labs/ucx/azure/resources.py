import urllib.parse
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from databricks.sdk.core import (
    ApiClient,
    AzureCliTokenSource,
    Config,
    CredentialsProvider,
    CredentialsStrategy,
    credentials_strategy,
)
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceConflict
from databricks.sdk.retries import retried

from databricks.labs.ucx.assessment.crawlers import logger

# https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles
_ROLES = {
    "STORAGE_BLOB_DATA_READER": "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1",
    "STORAGE_BLOB_DATA_CONTRIBUTOR": "ba92f5b4-2d11-453d-a403-e96b0029c9fe",
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

    @property
    def access_connector(self):
        return self._pairs.get("accessConnectors")

    def __eq__(self, other):
        if not isinstance(other, AzureResource):
            return NotImplemented
        return self._resource_id == other._resource_id

    def __repr__(self):
        properties = ["subscription_id", "resource_group", "storage_account", "container", "access_connector"]
        pairs = [f"{_}={getattr(self, _)}" for _ in properties]
        return f'AzureResource<{", ".join(pairs)}>'

    def __str__(self):
        return self._resource_id


class RawResource:
    def __init__(self, raw_resource: dict[str, Any]):
        if "id" not in raw_resource:
            raise KeyError("Raw resource must contain an 'id' field")
        self._id = AzureResource(raw_resource["id"])
        self._raw_resource = raw_resource

    @property
    def id(self) -> AzureResource:
        return self._id

    def get(self, key: str, default: Any) -> Any:
        return self._raw_resource.get(key, default)


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
class StorageAccount:
    id: AzureResource
    name: str
    location: str
    default_network_action: str  # "Unknown", "Deny" or "Allow"

    @classmethod
    def from_raw_resource(cls, raw: RawResource) -> "StorageAccount":
        if raw.id is None:
            raise KeyError(f"Missing id: {raw}")

        name = raw.get("name", "")
        if name == "":
            raise KeyError(f"Missing name: {raw}")

        location = raw.get("location", "")
        if location == "":
            raise KeyError(f"Missing location: {raw}")

        default_network_action = raw.get("properties", {}).get("networkAcls", {}).get("defaultAction", "Unknown")

        storage_account = cls(id=raw.id, name=name, location=location, default_network_action=default_network_action)
        return storage_account


@dataclass
class PrincipalSecret:
    client: Principal
    secret: str


@dataclass
class AzureRoleAssignment:
    id: str
    resource: AzureResource
    scope: AzureResource
    principal: Principal
    role_name: str
    role_type: str
    role_permissions: list[str]


@dataclass
class AzureRoleDetails:
    role_name: str | None
    role_type: str
    role_permissions: list[str]


@dataclass
class AccessConnector:
    id: AzureResource
    name: str
    location: str
    provisioning_state: str  # https://learn.microsoft.com/en-us/rest/api/databricks/access-connectors/get?view=rest-databricks-2023-05-01&tabs=HTTP#provisioningstate
    identity_type: str  # SystemAssigned or UserAssigned
    principal_id: str
    managed_identity_type: str | None = None  # str when identity_type is UserAssigned
    client_id: str | None = None  # str when identity_type is UserAssigned
    tenant_id: str | None = None  # str when identity_type is SystemAssigned
    tags: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_raw_resource(cls, raw: RawResource) -> "AccessConnector":
        if raw.id is None:
            raise KeyError(f"Missing id: {raw}")

        name = raw.get("name", "")
        if name == "":
            raise KeyError(f"Missing name: {raw}")

        location = raw.get("location", "")
        if location == "":
            raise KeyError(f"Missing location: {raw}")

        provisioning_state = raw.get("properties", {}).get("provisioningState", "")
        if provisioning_state == "":
            raise KeyError(f"Missing provisioning state: {raw}")

        identity = raw.get("identity", {})
        identity_type = identity.get("type")
        if identity_type == "UserAssigned":
            if len(identity.keys()) > 1:
                raise KeyError(f"Multiple user assigned identities: {identity.keys()}")
            if len(identity.keys()) == 0:
                raise KeyError(f"No user assigned identity: {identity.keys()}")
            managed_identity_id = list(identity.keys())[0]
            principal_id = identity[managed_identity_id]["principalId"]
            client_id = identity[managed_identity_id]["clientId"]
            tenant_id = None
        elif identity_type == "SystemAssigned":
            principal_id = identity["principalId"]
            managed_identity_id = client_id = None
            tenant_id = identity["tenantId"]
        else:
            raise KeyError(f"Unsupported identity type: {identity_type}")

        access_connector = cls(
            id=raw.id,
            name=name,
            location=location,
            provisioning_state=provisioning_state,
            identity_type=identity_type,
            principal_id=principal_id,
            managed_identity_type=managed_identity_id,
            client_id=client_id,
            tenant_id=tenant_id,
            tags=raw.get("tags", {}),
        )
        return access_connector


class AzureAPIClient:
    def __init__(self, host_endpoint: str, service_endpoint: str):
        self.api_client = ApiClient(
            Config(
                host=host_endpoint,
                credentials_strategy=self._strategy_for(service_endpoint),
            )
        )
        self._token_source = AzureCliTokenSource(host_endpoint)

    @staticmethod
    def _strategy_for(endpoint: str) -> CredentialsStrategy:
        @credentials_strategy("azure-cli", ["host"])
        def _credentials(_: Config) -> CredentialsProvider:
            token_source = AzureCliTokenSource(endpoint)

            def inner() -> dict[str, str]:
                token = token_source.token()
                return {"Authorization": f"{token.token_type} {token.access_token}"}

            return inner

        return _credentials

    def get(self, path: str, api_version: str | None = None, query: dict[str, str] | None = None):
        headers = {"Accept": "application/json"}
        _query: dict[str, str] = query or {}
        if api_version is not None:
            _query["api-version"] = api_version
        return self.api_client.do("GET", path, query=_query, headers=headers)

    def put(self, path: str, api_version: str | None = None, body: dict | None = None):
        headers = {"Content-Type": "application/json"}
        query: dict[str, str] = {}
        if api_version is not None:
            query["api-version"] = api_version
        if body is not None:
            return self.api_client.do("PUT", path, query=query, headers=headers, body=body)
        return None

    def post(self, path: str, body: dict | None = None):
        headers = {"Content-Type": "application/json"}
        query: dict[str, str] = {}
        if body is not None:
            return self.api_client.do("POST", path, query=query, headers=headers, body=body)
        return self.api_client.do("POST", path, query=query, headers=headers)

    def delete(self, path: str, api_version: str | None = None):
        # this method is added only to be used in int test to delete the application once tests pass
        headers = {"Content-Type": "application/json"}
        query: dict[str, str] = {}
        if api_version is not None:
            query["api-version"] = api_version
        return self.api_client.do("DELETE", path, query=query, headers=headers)

    def token(self):
        return self._token_source.token()


class AzureResources:
    def __init__(self, azure_mgmt: AzureAPIClient, azure_graph: AzureAPIClient, include_subscriptions=None):
        if not include_subscriptions:
            include_subscriptions = []
        self._mgmt = azure_mgmt
        self._graph = azure_graph
        self._include_subscriptions = include_subscriptions
        self._role_definitions = {}  # type: dict[str, AzureRoleDetails]
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
        principal_secret = PrincipalSecret(
            Principal(client_id, display_name, object_id, principal_type, directory_id), secret
        )
        logger.info(
            f"Created service principal ({principal_secret.client.client_id}) with access to used storage accounts: "
            + principal_secret.client.display_name
        )
        return principal_secret

    def delete_service_principal(self, principal_id: str, *, safe: bool = False):
        """Delete the service principal.

        Parameters
        ----------
        principal_id : str
            The principal id to delete.
        safe : bool, optional (default: True)
            If True, will not raise an error if the service principal does not exists.

        Raises
        ------
        NotFound :
            If the principal is not found.
        PermissionDenied :
            If missing permission to delete the service principal
        """
        try:
            self._graph.delete(f"/v1.0/applications(appId='{principal_id}')")
        except PermissionDenied:
            msg = f"User doesnt have permission to delete application {principal_id}"
            logger.error(msg)
            raise PermissionDenied(msg) from None
        except NotFound:
            if safe:
                return
            raise

    def _log_permission_denied_error_for_storage_permission(self, path: str) -> None:
        logger.error(
            "Permission denied. Please run this cmd under the identity of a user who has "
            f"create service principal permission: {path}"
        )

    def get_storage_permission(
        self,
        storage_account: StorageAccount,
        role_guid: str,
        *,
        timeout: timedelta = timedelta(seconds=1),
    ) -> AzureRoleAssignment | None:
        """Get a storage permission.

        Parameters
        ----------
        storage_account : StorageAccount
            The storage account to get the permission for.
        role_guid : str
            The role guid to get the permission for.
        timeout : timedelta, optional (default: timedelta(seconds=1))
            The timeout to wait for the permission to be found.

        Raises
        ------
        PermissionDenied :
            If user is missing permission to get the storage permission.
        """
        retry = retried(on=[NotFound], timeout=timeout)
        path = f"{storage_account.id}/providers/Microsoft.Authorization/roleAssignments/{role_guid}"
        try:
            response = retry(self._mgmt.get)(path, "2022-04-01")
            assignment = self._role_assignment(response, str(storage_account.id))
            return assignment
        except TimeoutError:  # TimeoutError is raised by retried
            logger.warning(f"Storage permission not found: {path}")  # not found because retry on NotFound
            return None
        except PermissionDenied:
            self._log_permission_denied_error_for_storage_permission(path)
            raise

    def _get_storage_permissions(
        self,
        principal_id: str,
        storage_account: StorageAccount,
    ) -> Iterable[AzureRoleAssignment]:
        """Get storage permissions for a principal.

        Parameters
        ----------
        principal_id : str
            The principal id to get the storage permissions for.
        storage_account : StorageAccount
            The storage account to get the permission for.

        Yields
        ------
        AzureRoleAssignment :
            The role assignment

        Raises
        ------
        PermissionDenied :
            If user is missing permission to get the storage permission.
        """
        path = (
            f"{storage_account.id}/providers/Microsoft.Authorization/roleAssignments"
            f"?$filter=principalId%20eq%20'{principal_id}'"
        )
        try:
            response = self._mgmt.get(path, "2022-04-01")
        except PermissionDenied:
            self._log_permission_denied_error_for_storage_permission(path)
            raise

        for role_assignment in response.get("value", []):
            assignment = self._role_assignment(role_assignment, str(storage_account.id))
            if assignment:
                yield assignment

    def apply_storage_permission(
        self, principal_id: str, storage_account: StorageAccount, role_name: str, role_guid: str
    ):
        role_id = _ROLES[role_name]
        path = f"{storage_account.id}/providers/Microsoft.Authorization/roleAssignments/{role_guid}"
        try:
            role_definition_id = f"/subscriptions/{storage_account.id.subscription_id}/providers/Microsoft.Authorization/roleDefinitions/{role_id}"
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
                f"Role assignment already exists for role {role_guid} on storage {storage_account.name}"
                f" for spn {principal_id}."
            )
        except PermissionDenied:
            self._log_permission_denied_error_for_storage_permission(path)
            raise

    def _delete_storage_permission(
        self, principal_id: str, storage_account: StorageAccount, *, safe: bool = False
    ) -> None:
        """See meth:delete_storage_permission"""
        try:
            storage_permissions = list(self._get_storage_permissions(principal_id, storage_account))
        except NotFound:
            if safe:
                return
            raise
        permission_denied_ids = []
        for permission in storage_permissions:
            try:
                self._mgmt.delete(permission.id, "2022-04-01")
            except PermissionDenied:
                self._log_permission_denied_error_for_storage_permission(permission.id)
                permission_denied_ids.append(permission.id)
            except NotFound:
                continue  # Somehow deleted right in-between getting and deleting
        if permission_denied_ids:
            raise PermissionDenied(
                f"Permission denied for deleting role assignments: {', '.join(permission_denied_ids)}"
            )

    def delete_storage_permission(
        self, principal_id: str, *storage_accounts: StorageAccount, safe: bool = False
    ) -> None:
        """Delete storage permission(s) for a principal

        Parameters
        ----------
        principal_id : str
            The principal id to delete the role assignment(s) for.
        storage_accounts : StorageAccount
            The storage account(s) to delete permission for.
        safe : bool, optional (default: False)
            If True, will not raise an exception if no role assignment are found.

        Raises
        ------
        PermissionDenied :
            If user is missing permission to get the storage permission.
        """
        for storage_account in storage_accounts:
            self._delete_storage_permission(principal_id, storage_account, safe=safe)

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

    def storage_accounts(self) -> Iterable[StorageAccount]:
        for subscription in self.subscriptions():
            logger.info(f"Checking in subscription {subscription.name} for storage accounts")
            path = f"/subscriptions/{subscription.subscription_id}/providers/Microsoft.Storage/storageAccounts"
            for response in self._mgmt.get(path, "2023-01-01").get("value", []):
                try:
                    storage_account = StorageAccount.from_raw_resource(RawResource(response))
                except KeyError:
                    logger.warning(f"Failed parsing storage account: {response}")
                else:
                    yield storage_account

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
            principal_type = role_assignment.get("properties", {}).get("principalType")
            if not principal_type or principal_type not in principal_types:
                continue
            assignment = self._role_assignment(role_assignment, resource_id)
            if not assignment:
                continue
            yield assignment

    def _role_assignment(self, role_assignment: dict, resource_id: str) -> AzureRoleAssignment | None:
        id_ = role_assignment.get("id")
        if not id_:
            return None
        assignment_properties = role_assignment.get("properties", {})
        principal_type = assignment_properties.get("principalType")
        if not principal_type:
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
        role_details = self._role_name(role_definition_id)
        role_name = role_details.role_name
        if not role_name:
            return None
        principal = self._get_principal(principal_id)
        if not principal:
            return None
        if scope == "/":
            scope = resource_id
        return AzureRoleAssignment(
            id=id_,
            resource=AzureResource(resource_id),
            scope=AzureResource(scope),
            principal=principal,
            role_name=role_name,
            role_type=role_details.role_type,
            role_permissions=role_details.role_permissions,
        )

    def _role_name(self, role_definition_id) -> AzureRoleDetails:
        if role_definition_id not in self._role_definitions:
            role_definition = self._mgmt.get(role_definition_id, "2022-04-01")
            definition_properties = role_definition.get("properties", {})
            role_name = definition_properties.get("roleName")
            if not role_name:
                return AzureRoleDetails(role_name=None, role_type='BuiltInRole', role_permissions=[])
            role_type = definition_properties.get("type", "BuiltInRole")
            role_permissions = []
            if role_type == 'CustomRole':
                role_permissions_list = definition_properties.get("permissions", [])
                for each_role_permissions in role_permissions_list:
                    role_permissions = each_role_permissions.get("actions", []) + each_role_permissions.get(
                        "dataActions", []
                    )
            self._role_definitions[role_definition_id] = AzureRoleDetails(
                role_name=role_name, role_type=role_type, role_permissions=role_permissions
            )
        return self._role_definitions[role_definition_id]

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

    def get_access_connector(self, subscription_id: str, resource_group_name: str, name: str) -> AccessConnector | None:
        """Get an access connector.

        Docs:
            https://learn.microsoft.com/en-us/rest/api/databricks/access-connectors/get?view=rest-databricks-2023-05-01&tabs=HTTP
        """
        url = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Databricks/accessConnectors/{name}"
        response = self._mgmt.get(url, api_version="2023-05-01")
        raw = RawResource(response)
        try:
            access_connector = AccessConnector.from_raw_resource(raw)
        except KeyError:
            logger.warning(f"Tried getting non-existing access connector: {url}")
            access_connector = None
        return access_connector

    def list_resources(self, subscription: AzureSubscription, resource_type: str) -> Iterable[RawResource]:
        """List all resources of a type within subscription"""
        query = {"api-version": "2020-06-01", "$filter": f"resourceType eq '{resource_type}'"}
        while True:
            res = self._mgmt.get(f"/subscriptions/{subscription.subscription_id}/resources", query=query)
            for resource in res["value"]:
                try:
                    yield RawResource(resource)
                except KeyError:
                    logger.warning(f"Could not parse resource: {resource}")

            next_link = res.get("nextLink", None)
            if not next_link:
                break
            parsed_link = urllib.parse.urlparse(next_link)
            query = dict(urllib.parse.parse_qsl(parsed_link.query))

    def access_connectors(self) -> Iterable[AccessConnector]:
        """List all access connector within subscription

        Docs:
            https://learn.microsoft.com/en-us/rest/api/databricks/access-connectors/list-by-subscription?view=rest-databricks-2023-05-01&tabs=HTTP
        """
        for subscription in self.subscriptions():
            for raw in self.list_resources(subscription, "Microsoft.Databricks/accessConnectors"):
                try:
                    yield AccessConnector.from_raw_resource(raw)
                except KeyError:
                    logger.warning(f"Could not parse access connector: {raw}")

    def create_or_update_access_connector(
        self,
        subscription_id: str,
        resource_group_name: str,
        name: str,
        location: str,
        tags: dict[str, str] | None,
        *,
        wait_for_provisioning: bool = False,
        wait_for_provisioning_timeout_in_seconds: int = 300,
    ) -> AccessConnector:
        """Create access connector.

        Docs:
            https://learn.microsoft.com/en-us/rest/api/databricks/access-connectors/create-or-update?view=rest-databricks-2023-05-01&tabs=HTTP
        """
        url = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Databricks/accessConnectors/{name}"
        body = {
            "location": location,
            "identity": {"type": "SystemAssigned"},
        }
        if tags is not None:
            body["tags"] = tags
        self._mgmt.put(url, api_version="2023-05-01", body=body)

        access_connector = self.get_access_connector(subscription_id, resource_group_name, name)

        start_time = time.time()
        if access_connector is None or (wait_for_provisioning and access_connector.provisioning_state != "Succeeded"):
            if time.time() - start_time > wait_for_provisioning_timeout_in_seconds:
                raise TimeoutError(f"Timeout waiting for creating or updating access connector: {url}")
            time.sleep(5)

            access_connector = self.get_access_connector(subscription_id, resource_group_name, name)
            assert access_connector is not None

        return access_connector

    def delete_access_connector(self, url: str) -> None:
        """Delete an access connector.

        Docs:
            https://learn.microsoft.com/en-us/rest/api/databricks/access-connectors/delete?view=rest-databricks-2023-05-01&tabs=HTTP
        """
        self._mgmt.delete(url, api_version="2023-05-01")
