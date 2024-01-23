import base64
import csv
import dataclasses
import io
import json
import re
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
from databricks.sdk.service.catalog import Privilege
from databricks.sdk.service.compute import ClusterSource, Policy
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.assessment.crawlers import (
    _CLIENT_ENDPOINT_LENGTH,
    _SECRET_LIST_LENGTH,
    _SECRET_PATTERN,
    _STORAGE_ACCOUNT_EXTRACT_PATTERN,
    _azure_sp_conf_present_check,
    logger,
)
from databricks.labs.ucx.assessment.jobs import JobsMixin
from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations


@dataclass
class AzureServicePrincipalInfo:
    # fs.azure.account.oauth2.client.id
    application_id: str
    # fs.azure.account.oauth2.client.secret: {{secrets/${local.secret_scope}/${local.secret_key}}}
    secret_scope: str
    # fs.azure.account.oauth2.client.secret: {{secrets/${local.secret_scope}/${local.secret_key}}}
    secret_key: str
    # fs.azure.account.oauth2.client.endpoint: "https://login.microsoftonline.com/${local.tenant_id}/oauth2/token"
    tenant_id: str
    # Azure Storage account to which the SP has been given access
    storage_account: str


class AzureServicePrincipalCrawler(CrawlerBase[AzureServicePrincipalInfo], JobsMixin):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "azure_service_principals", AzureServicePrincipalInfo)
        self._ws = ws

    def _crawl(self) -> Iterable[AzureServicePrincipalInfo]:
        all_relevant_service_principals = self._get_relevant_service_principals()
        deduped_service_principals = [dict(t) for t in {tuple(d.items()) for d in all_relevant_service_principals}]
        return list(self._assess_service_principals(deduped_service_principals))

    def _check_secret_and_get_application_id(self, secret_matched) -> str | None:
        split = secret_matched.group(1).split("/")
        if len(split) == _SECRET_LIST_LENGTH:
            secret_scope, secret_key = split[1], split[2]
            try:
                # Return the decoded secret value in string format
                secret = self._ws.secrets.get_secret(secret_scope, secret_key)
                assert secret.value is not None
                return base64.b64decode(secret.value).decode("utf-8")
            except NotFound:
                logger.warning(f'removed on the backend: {"/".join(split)}')
                return None
        return None

    def _get_azure_spn_tenant_id(self, config: dict, tenant_key: str) -> str | None:
        matching_key = [key for key in config.keys() if re.search(tenant_key, key)]
        if len(matching_key) > 0:
            matched = matching_key[0]
            if not matched:
                return None
            if re.search("spark_conf", matched):
                client_endpoint_list = config.get(matched, {}).get("value", "").split("/")
            else:
                client_endpoint_list = config.get(matched, "").split("/")
            if len(client_endpoint_list) == _CLIENT_ENDPOINT_LENGTH:
                return client_endpoint_list[3]
        return None

    def _get_azure_spn_list(self, config: dict) -> list:
        spn_list = []
        secret_scope, secret_key, tenant_id, storage_account = None, None, None, None
        matching_key_list = [key for key in config.keys() if "fs.azure.account.oauth2.client.id" in key]
        if len(matching_key_list) > 0:
            for key in matching_key_list:
                storage_account_match = re.search(_STORAGE_ACCOUNT_EXTRACT_PATTERN, key)
                if re.search("spark_conf", key):
                    spn_application_id = config.get(key, {}).get("value")
                else:
                    spn_application_id = config.get(key)
                if not spn_application_id:
                    continue
                secret_matched = re.search(_SECRET_PATTERN, spn_application_id)
                if secret_matched:
                    spn_application_id = self._check_secret_and_get_application_id(secret_matched)
                    if not spn_application_id:
                        continue
                    secret_scope, secret_key = (
                        secret_matched.group(1).split("/")[1],
                        secret_matched.group(1).split("/")[2],
                    )
                if storage_account_match:
                    storage_account = storage_account_match.group(1).strip(".")
                    tenant_key = "fs.azure.account.oauth2.client.endpoint." + storage_account
                else:
                    tenant_key = "fs.azure.account.oauth2.client.endpoint"
                tenant_id = self._get_azure_spn_tenant_id(config, tenant_key)

                spn_application_id = "" if spn_application_id is None else spn_application_id
                secret_scope = "" if secret_scope is None else secret_scope
                secret_key = "" if secret_key is None else secret_key
                tenant_id = "" if tenant_id is None else tenant_id
                storage_account = "" if storage_account is None else storage_account
                spn_list.append(
                    {
                        "application_id": spn_application_id,
                        "secret_scope": secret_scope,
                        "secret_key": secret_key,
                        "tenant_id": tenant_id,
                        "storage_account": storage_account,
                    }
                )
        return spn_list

    def _get_relevant_service_principals(self) -> list:
        relevant_service_principals = []
        temp_list = self._list_all_cluster_with_spn_in_spark_conf()
        if temp_list:
            relevant_service_principals += temp_list
        temp_list = self._list_all_pipeline_with_spn_in_spark_conf()
        if temp_list:
            relevant_service_principals += temp_list
        temp_list = self._list_all_jobs_with_spn_in_spark_conf()
        if temp_list:
            relevant_service_principals += temp_list
        temp_list = self._list_all_spn_in_sql_warehouses_spark_conf()
        if temp_list:
            relevant_service_principals += temp_list
        return relevant_service_principals

    def _list_all_jobs_with_spn_in_spark_conf(self) -> list:
        azure_spn_list = []
        all_jobs = list(self._ws.jobs.list(expand_tasks=True))
        all_clusters_by_id = {c.cluster_id: c for c in self._ws.clusters.list()}

        for _job, cluster_config in self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
            azure_spn_list += self._get_azure_spn_with_data_access(cluster_config)

        return azure_spn_list

    def _list_all_cluster_with_spn_in_spark_conf(self) -> list:
        azure_spn_list = []

        for cluster_config in self._ws.clusters.list():
            if cluster_config.cluster_source != ClusterSource.JOB:
                azure_spn_list += self._get_azure_spn_with_data_access(cluster_config)

        return azure_spn_list

    def _get_azure_spn_with_data_access(self, cluster_config):
        azure_spn_list = []

        if cluster_config.spark_conf:
            if _azure_sp_conf_present_check(cluster_config.spark_conf):
                temp_list = self._get_azure_spn_list(cluster_config.spark_conf)
                if temp_list:
                    azure_spn_list += temp_list

        if cluster_config.policy_id:
            policy = self._safe_get_cluster_policy(cluster_config.policy_id)

            if policy is None:
                return azure_spn_list

            if policy.definition:
                if _azure_sp_conf_present_check(json.loads(policy.definition)):
                    temp_list = self._get_azure_spn_list(json.loads(policy.definition))
                    if temp_list:
                        azure_spn_list += temp_list

            if policy.policy_family_definition_overrides:
                if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                    temp_list = self._get_azure_spn_list(json.loads(policy.policy_family_definition_overrides))
                    if temp_list:
                        azure_spn_list += temp_list

        return azure_spn_list

    def _safe_get_cluster_policy(self, policy_id: str) -> Policy | None:
        try:
            return self._ws.cluster_policies.get(policy_id)
        except NotFound:
            logger.warning(f"The cluster policy was deleted: {policy_id}")
            return None

    def _list_all_spn_in_sql_warehouses_spark_conf(self) -> list:
        warehouse_config_list = self._ws.warehouses.get_workspace_warehouse_config().data_access_config
        if warehouse_config_list:
            if len(warehouse_config_list) > 0:
                warehouse_config_dict = {config.key: config.value for config in warehouse_config_list}
                if warehouse_config_dict:
                    if _azure_sp_conf_present_check(warehouse_config_dict):
                        return self._get_azure_spn_list(warehouse_config_dict)
        return []

    def _list_all_pipeline_with_spn_in_spark_conf(self) -> list:
        azure_spn_list_with_data_access_from_pipeline = []
        for pipeline in self._ws.pipelines.list_pipelines():
            assert pipeline.pipeline_id is not None
            pipeline_info = self._ws.pipelines.get(pipeline.pipeline_id)
            assert pipeline_info.spec is not None
            pipeline_config = pipeline_info.spec.configuration
            if pipeline_config:
                if not _azure_sp_conf_present_check(pipeline_config):
                    continue
                temp_list = self._get_azure_spn_list(pipeline_config)
                if temp_list:
                    azure_spn_list_with_data_access_from_pipeline += temp_list
        return azure_spn_list_with_data_access_from_pipeline

    def _assess_service_principals(self, relevant_service_principals: list):
        for spn in relevant_service_principals:
            spn_info = AzureServicePrincipalInfo(
                application_id=spn.get("application_id"),
                secret_scope=spn.get("secret_scope"),
                secret_key=spn.get("secret_key"),
                tenant_id=spn.get("tenant_id"),
                storage_account=spn.get("storage_account"),
            )
            yield spn_info

    def snapshot(self) -> Iterable[AzureServicePrincipalInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[AzureServicePrincipalInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield AzureServicePrincipalInfo(*row)


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
            assignment_properties = role_assignment.get("properties", {})
            principal_type = assignment_properties.get("principalType")
            if not principal_type:
                continue
            if principal_type not in principal_types:
                continue
            principal_id = assignment_properties.get("principalId")
            if not principal_id:
                continue
            role_definition_id = assignment_properties.get("roleDefinitionId")
            if not role_definition_id:
                continue
            scope = assignment_properties.get("scope")
            if not scope:
                continue
            if role_definition_id not in self._role_definitions:
                role_definition = self._get_resource(role_definition_id, "2022-04-01")
                definition_properties = role_definition.get("properties", {})
                role_name: str = definition_properties.get("roleName")
                if not role_name:
                    continue
                self._role_definitions[role_definition_id] = role_name
            principal = self._get_principal(principal_id)
            if not principal:
                continue
            role_name = self._role_definitions[role_definition_id]
            if scope == "/":
                scope = resource_id
            yield AzureRoleAssignment(
                resource=AzureResource(resource_id),
                scope=AzureResource(scope),
                principal=principal,
                role_name=role_name,
            )


@dataclass
class StoragePermissionMapping:
    prefix: str
    client_id: str
    principal: str
    privilege: str


class AzureResourcePermissions:
    def __init__(self, ws: WorkspaceClient, azurerm: AzureResources, lc: ExternalLocations, folder: str | None = None):
        self._locations = lc
        self._azurerm = azurerm
        self._ws = ws
        self._field_names = [_.name for _ in dataclasses.fields(StoragePermissionMapping)]
        if not folder:
            folder = f"/Users/{ws.current_user.me().user_name}/.ucx"
        self._folder = folder
        self._levels = {
            "Storage Blob Data Contributor": Privilege.WRITE_FILES,
            "Storage Blob Data Owner": Privilege.WRITE_FILES,
            "Storage Blob Data Reader": Privilege.READ_FILES,
        }

    def _map_storage(self, storage: AzureResource) -> list[StoragePermissionMapping]:
        logger.info(f"Fetching role assignment for {storage.storage_account}")
        out = []
        for container in self._azurerm.containers(storage):
            for role_assignment in self._azurerm.role_assignments(str(container)):
                # one principal may be assigned multiple roles with overlapping dataActions, hence appearing
                # here in duplicates. hence, role name -> permission level is not enough for the perfect scenario.
                if role_assignment.role_name not in self._levels:
                    continue
                privilege = self._levels[role_assignment.role_name].value
                out.append(
                    StoragePermissionMapping(
                        prefix=f"abfss://{container.container}@{container.storage_account}.dfs.core.windows.net/",
                        client_id=role_assignment.principal.client_id,
                        principal=role_assignment.principal.display_name,
                        privilege=privilege,
                    )
                )
        return out

    def save_spn_permissions(self) -> str | None:
        used_storage_accounts = self._get_storage_accounts()
        if len(used_storage_accounts) == 0:
            logger.warning(
                "There are no external table present with azure storage account. "
                "Please check if assessment job is run"
            )
            return None
        storage_account_infos = []
        for storage in self._azurerm.storage_accounts():
            if storage.storage_account not in used_storage_accounts:
                continue
            for mapping in self._map_storage(storage):
                storage_account_infos.append(mapping)
        if len(storage_account_infos) == 0:
            logger.error("No storage account found in current tenant with spn permission")
            return None
        return self._save(storage_account_infos)

    def _save(self, storage_infos: list[StoragePermissionMapping]) -> str:
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, self._field_names)
        writer.writeheader()
        for storage_info in storage_infos:
            writer.writerow(dataclasses.asdict(storage_info))
        buffer.seek(0)
        return self._overwrite_mapping(buffer)

    def _overwrite_mapping(self, buffer) -> str:
        path = f"{self._folder}/azure_storage_account_info.csv"
        self._ws.workspace.upload(path, buffer, overwrite=True, format=ImportFormat.AUTO)
        return path

    def _get_storage_accounts(self) -> list[str]:
        external_locations = self._locations.snapshot()
        storage_accounts = []
        for location in external_locations:
            if location.location.startswith("abfss://"):
                start = location.location.index("@")
                end = location.location.index(".dfs.core.windows.net")
                storage_acct = location.location[start + 1 : end]
                if storage_acct not in storage_accounts:
                    storage_accounts.append(storage_acct)
        return storage_accounts
