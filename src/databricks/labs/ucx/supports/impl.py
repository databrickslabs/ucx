from collections.abc import Callable

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.inventory.types import PermissionsInventoryItem
from databricks.labs.ucx.supports.base import BaseSupport
from databricks.labs.ucx.supports.group_level import ScimSupport
from databricks.labs.ucx.supports.permissions import get_generic_support
from databricks.labs.ucx.supports.secrets import SecretsSupport
from databricks.labs.ucx.supports.sql import get_sql_support


class SupportsProvider:
    def __init__(self, ws: WorkspaceClient, num_threads: int, workspace_start_path: str):
        self._generic_support = get_generic_support(ws=ws, num_threads=num_threads, start_path=workspace_start_path)
        self._secrets_support = SecretsSupport(ws=ws)
        self._scim_support = ScimSupport(ws)
        self._sql_support = get_sql_support(ws=ws)

    def get_crawler_tasks(self) -> list[Callable[..., PermissionsInventoryItem | None]]:
        for support in [self._generic_support, self._secrets_support, self._scim_support, self._sql_support]:
            yield from support.get_crawler_tasks()

    @property
    def supports(self) -> dict[str, BaseSupport]:
        return {
            # SCIM-based API
            "entitlements": self._scim_support,
            "roles": self._scim_support,
            # generic API
            "clusters": self._generic_support,
            "cluster_policies": self._generic_support,
            "instance_pools": self._generic_support,
            "sql_warehouses": self._generic_support,
            "jobs": self._generic_support,
            "pipelines": self._generic_support,
            "experiments": self._generic_support,
            "registered_models": self._generic_support,
            "tokens": self._generic_support,
            "passwords": self._generic_support,
            # workspace objects
            "notebooks": self._generic_support,
            "files": self._generic_support,
            "directories": self._generic_support,
            "repos": self._generic_support,
            # SQL API
            "alerts": self._sql_support,
            "queries": self._sql_support,
            "dashboards": self._sql_support,
            # secrets API
            "secrets": self._secrets_support,
        }
