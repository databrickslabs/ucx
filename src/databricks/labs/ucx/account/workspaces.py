import base64
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import AzureCliTokenSource, Config, DatabricksError
from databricks.sdk.service.provisioning import PricingTier, Workspace
from requests.exceptions import ConnectionError

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import AccountConfig

logger = logging.getLogger(__name__)


@dataclass
class AzureSubscription:
    name: str
    subscription_id: str
    tenant_id: str


class AzureWorkspaceLister:
    def __init__(self, cfg: Config):
        endpoint = cfg.arm_environment.resource_manager_endpoint
        self._token_source = AzureCliTokenSource(endpoint)
        self._endpoint = endpoint

    def _get(self, path: str, *, api_version=None) -> dict:
        token = self._token_source.token()
        headers = {"Authorization": f"{token.token_type} {token.access_token}"}
        return requests.get(
            self._endpoint + path, headers=headers, params={"api-version": api_version}, timeout=10
        ).json()

    def _all_subscriptions(self):
        for sub in self._get("/subscriptions", api_version="2022-12-01").get("value", []):
            yield AzureSubscription(
                name=sub["displayName"], subscription_id=sub["subscriptionId"], tenant_id=sub["tenantId"]
            )

    def _tenant_id(self):
        token = self._token_source.token()
        _, payload, _ = token.access_token.split(".")
        b64_decoded = base64.standard_b64decode(payload + "==").decode("utf8")
        claims = json.loads(b64_decoded)
        return claims["tid"]

    def current_tenant_subscriptions(self):
        tenant_id = self._tenant_id()
        for sub in self._all_subscriptions():
            if sub.tenant_id != tenant_id:
                continue
            yield sub

    def subscriptions_name_to_id(self):
        return {sub.name: sub.subscription_id for sub in self.current_tenant_subscriptions()}

    def list_workspaces(self, subscription_id):
        endpoint = f"/subscriptions/{subscription_id}/providers/Microsoft.Databricks/workspaces"
        sku_tiers = {
            "premium": PricingTier.PREMIUM,
            "enterprise": PricingTier.ENTERPRISE,
            "standard": PricingTier.STANDARD,
            "unknown": PricingTier.UNKNOWN,
        }
        items = self._get(endpoint, api_version="2023-02-01").get("value", [])
        for item in sorted(items, key=lambda _: _["name"].lower()):
            properties = item["properties"]
            if properties["provisioningState"] != "Succeeded":
                continue
            if "workspaceUrl" not in properties:
                continue
            parameters = properties.get("parameters", {})
            workspace_url = properties["workspaceUrl"]
            tags = item.get("tags", {})
            if "AzureSubscriptionID" not in tags:
                tags["AzureSubscriptionID"] = subscription_id
            if "AzureResourceGroup" not in tags:
                tags["AzureResourceGroup"] = item["id"].split("resourceGroups/")[1].split("/")[0]
            yield Workspace(
                cloud="azure",
                location=item["location"],
                workspace_name=item["name"],
                workspace_id=int(properties["workspaceId"]),
                workspace_status_message=properties["provisioningState"],
                deployment_name=workspace_url.replace(".azuredatabricks.net", ""),
                pricing_tier=sku_tiers.get(item.get("sku", {"name": None})["name"], None),
                # These fields are just approximation for the fields with same meaning in AWS and GCP
                storage_configuration_id=parameters.get("storageAccountName", {"value": None})["value"],
                network_id=parameters.get("customVirtualNetworkId", {"value": None})["value"],
                custom_tags=tags,
            )


class Workspaces:
    _tlds: ClassVar[dict[str, str]] = {
        "aws": "cloud.databricks.com",
        "azure": "azuredatabricks.net",
        "gcp": "gcp.databricks.com",
    }

    def __init__(self, cfg: AccountConfig):
        self._ac = cfg.to_account_client()
        self._cfg = cfg

    def configured_workspaces(self):
        for workspace in self._all_workspaces():
            if self._cfg.include_workspace_names:
                if workspace.workspace_name not in self._cfg.include_workspace_names:
                    logger.debug(
                        f"skipping {workspace.name} ({workspace.workspace_id} because its not explicitly included"
                    )
                    continue
            yield workspace

    def client_for(self, workspace: Workspace) -> WorkspaceClient:
        config = self._ac.config.as_dict()
        # copy current config and swap with a host relevant to a workspace
        config["host"] = f"https://{workspace.deployment_name}.{self._tlds[workspace.cloud]}"
        return WorkspaceClient(**config, product="ucx", product_version=__version__)

    def _all_workspaces(self):
        if self._ac.config.is_azure:
            yield from self._azure_workspaces()
        else:
            yield from self._native_workspaces()

    def _native_workspaces(self):
        yield from self._ac.workspaces.list()

    def _azure_workspaces(self):
        azure_lister = AzureWorkspaceLister(self._ac.config)
        for sub in azure_lister.current_tenant_subscriptions():
            if self._cfg.include_azure_subscription_ids:
                if sub.subscription_id not in self._cfg.include_azure_subscription_ids:
                    logger.debug(f"skipping {sub.name} ({sub.subscription_id} because its not explicitly included")
                    continue
            if self._cfg.include_azure_subscription_names:
                if sub.name not in self._cfg.include_azure_subscription_names:
                    logger.debug(f"skipping {sub.name} ({sub.subscription_id} because its not explicitly included")
                    continue
            for workspace in azure_lister.list_workspaces(sub.subscription_id):
                if "AzureSubscription" not in workspace.custom_tags:
                    workspace.custom_tags["AzureSubscription"] = sub.name
                yield workspace


def main():
    logger.setLevel("INFO")

    config_file = Path.home() / ".ucx/config.yml"
    cfg = AccountConfig.from_file(config_file)
    wss = Workspaces(cfg)

    for workspace in wss.configured_workspaces():
        ws = wss.client_for(workspace)

        metastore_id = "NOT ASSIGNED"
        default_catalog = "hive_metastore"
        try:
            metastore = ws.metastores.current()
            default_catalog = metastore.default_catalog_name
            metastore_id = metastore.metastore_id
        except DatabricksError:
            pass
        except ConnectionError:
            logger.warning(f"Private DNS for {workspace.workspace_name} is not yet supported?..")

        logger.info(
            f"workspace: {workspace.workspace_name}: "
            f"metastore {metastore_id}, "
            f"default catalog: {default_catalog}"
        )


if __name__ == "__main__":
    main()
