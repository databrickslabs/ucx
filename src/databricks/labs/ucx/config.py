import dataclasses
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.core import Config

from databricks.labs.ucx.__about__ import __version__

__all__ = ["GroupsConfig", "AccountConfig", "WorkspaceConfig"]


# TODO: flatten down
@dataclass
class GroupsConfig:
    selected: list[str] | None = None
    auto: bool | None = None
    backup_group_prefix: str | None = "db-temp-"

    def __post_init__(self):
        if not self.selected and self.auto is None:
            msg = "Either selected or auto must be set"
            raise ValueError(msg)
        if self.selected and self.auto is False:
            msg = "No selected groups provided, but auto-collection is disabled"
            raise ValueError(msg)

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(**raw)


@dataclass
class ConnectConfig:
    # Keep all the fields in sync with databricks.sdk.core.Config
    host: str | None = None
    account_id: str | None = None
    token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    azure_client_id: str | None = None
    azure_tenant_id: str | None = None
    azure_client_secret: str | None = None
    azure_environment: str | None = None
    cluster_id: str | None = None
    profile: str | None = None
    debug_headers: bool | None = False
    rate_limit: int | None = None
    max_connections_per_pool: int | None = None
    max_connection_pools: int | None = None

    @staticmethod
    def from_databricks_config(cfg: Config) -> "ConnectConfig":
        return ConnectConfig(
            host=cfg.host,
            token=cfg.token,
            client_id=cfg.client_id,
            client_secret=cfg.client_secret,
            azure_client_id=cfg.azure_client_id,
            azure_tenant_id=cfg.azure_tenant_id,
            azure_client_secret=cfg.azure_client_secret,
            azure_environment=cfg.azure_environment,
            cluster_id=cfg.cluster_id,
            profile=cfg.profile,
            debug_headers=cfg.debug_headers,
            rate_limit=cfg.rate_limit,
            max_connection_pools=cfg.max_connection_pools,
            max_connections_per_pool=cfg.max_connections_per_pool,
        )

    def to_databricks_config(self):
        return Config(
            host=self.host,
            account_id=self.account_id,
            token=self.token,
            client_id=self.client_id,
            client_secret=self.client_secret,
            azure_client_id=self.azure_client_id,
            azure_tenant_id=self.azure_tenant_id,
            azure_client_secret=self.azure_client_secret,
            azure_environment=self.azure_environment,
            cluster_id=self.cluster_id,
            profile=self.profile,
            debug_headers=self.debug_headers,
            rate_limit=self.rate_limit,
            max_connection_pools=self.max_connection_pools,
            max_connections_per_pool=self.max_connections_per_pool,
            product="ucx",
            product_version=__version__,
        )

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(**raw)


# Used to set the right expectation about configuration file schema
_CONFIG_VERSION = 1

T = TypeVar("T")


class _Config(Generic[T]):
    connect: ConnectConfig | None = None

    @classmethod
    @abstractmethod
    def from_dict(cls, raw: dict) -> T:
        ...

    @classmethod
    def from_bytes(cls, raw: str) -> T:
        from yaml import safe_load

        raw = safe_load(raw)
        return cls.from_dict({} if not raw else raw)

    @classmethod
    def from_file(cls, config_file: Path) -> T:
        return cls.from_bytes(config_file.read_text())

    @classmethod
    def _verify_version(cls, raw):
        stored_version = raw.pop("version", None)
        if stored_version != _CONFIG_VERSION:
            msg = (
                f"Unsupported config version: {stored_version}. "
                f"UCX v{__version__} expects config version to be {_CONFIG_VERSION}"
            )
            raise ValueError(msg)

    def __post_init__(self):
        if self.connect is None:
            self.connect = ConnectConfig()

    def to_databricks_config(self) -> Config:
        connect = self.connect
        if connect is None:
            # default empty config
            connect = ConnectConfig()
        return connect.to_databricks_config()

    def as_dict(self) -> dict[str, Any]:
        from dataclasses import fields, is_dataclass

        def inner(x):
            if is_dataclass(x):
                result = []
                for f in fields(x):
                    value = inner(getattr(x, f.name))
                    if not value:
                        continue
                    result.append((f.name, value))
                return dict(result)
            return x

        serialized = inner(self)
        serialized["version"] = _CONFIG_VERSION
        return serialized


@dataclass
class AccountConfig(_Config["AccountConfig"]):
    # At least account console hostname and Databricks Account ID are required for the wide
    # account-level installation. These values we cannot determine automatically. For Azure,
    # it is required to run `az login` before the setup. For AWS, administrators may need
    # to specify username and password credentials to speak to non-UC workspaces, otherwise
    # `databricks auth login` from Databricks CLI is enough.
    #
    # The same configuration is optional on the workspace level, as we rely on the runtime-native
    # authentication of Databricks SDK for Python.
    connect: ConnectConfig

    # Inventory database holds the working state of UCX within hive_metastore catalogs
    # across all deployed workspaces. Once configured, it cannot be changed. In the rare
    # circumstances when we need to change the database name, we have to destroy all
    # databases with the previous name in each workspace and restart the assessment steps.
    inventory_database: str = "ucx"

    # Migrate only specific set of Databricks Workspaces. All by default. Keep in mind,
    # that the workspace name (what is seen in the Databricks Account Console) may be
    # different from the deployment name (what is seen as part of the workspace URL).
    include_workspace_names: list[str] = dataclasses.field(default_factory=list)
    include_azure_subscription_ids: list[str] = dataclasses.field(default_factory=list)
    include_azure_subscription_names: list[str] = dataclasses.field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: dict):
        cls._verify_version(raw)
        connect = ConnectConfig.from_dict(raw.pop("connect", {}))
        return cls(connect=connect, **raw)

    def to_account_client(self) -> AccountClient:
        return AccountClient(config=self.to_databricks_config())


@dataclass
class WorkspaceConfig(_Config["WorkspaceConfig"]):
    inventory_database: str
    groups: GroupsConfig
    instance_pool_id: str = None
    warehouse_id: str = None
    connect: ConnectConfig | None = None
    num_threads: int | None = 10
    log_level: str | None = "INFO"

    # Starting path for notebooks and directories crawler
    workspace_start_path: str = "/"

    @classmethod
    def from_dict(cls, raw: dict):
        cls._verify_version(raw)
        return cls(
            inventory_database=raw.get("inventory_database"),
            groups=GroupsConfig.from_dict(raw.get("groups", {})),
            connect=ConnectConfig.from_dict(raw.get("connect", {})),
            instance_pool_id=raw.get("instance_pool_id", None),
            warehouse_id=raw.get("warehouse_id", None),
            num_threads=raw.get("num_threads", 10),
            log_level=raw.get("log_level", "INFO"),
        )

    def to_workspace_client(self) -> WorkspaceClient:
        return WorkspaceClient(config=self.to_databricks_config())
