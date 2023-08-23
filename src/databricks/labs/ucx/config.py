from databricks.sdk.core import Config
from pydantic import RootModel
from pydantic.dataclasses import dataclass

from databricks.labs.ucx.__about__ import __version__


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


@dataclass
class InventoryConfig:
    catalog: str
    database: str
    warehouse_id: str


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
    max_connection_pools: int | None = None
    max_connections_per_pool: int | None = None

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


@dataclass
class MigrationConfig:
    inventory: InventoryConfig
    with_table_acls: bool
    groups: GroupsConfig
    connect: ConnectConfig | None = None
    num_threads: int | None = 4
    log_level: str | None = "INFO"

    def __post_init__(self):
        if self.with_table_acls:
            msg = "Table ACLS are not yet implemented"
            raise NotImplementedError(msg)

    def to_databricks_config(self) -> Config:
        connect = self.connect
        if connect is None:
            # default empty config
            connect = ConnectConfig()
        return Config(
            host=connect.host,
            account_id=connect.account_id,
            token=connect.token,
            client_id=connect.client_id,
            client_secret=connect.client_secret,
            azure_client_id=connect.azure_client_id,
            azure_tenant_id=connect.azure_tenant_id,
            azure_client_secret=connect.azure_client_secret,
            azure_environment=connect.azure_environment,
            cluster_id=connect.cluster_id,
            profile=connect.profile,
            debug_headers=connect.debug_headers,
            rate_limit=connect.rate_limit,
            max_connection_pools=connect.max_connection_pools,
            max_connections_per_pool=connect.max_connections_per_pool,
            product="ucx",
            product_version=__version__,
        )

    def to_json(self) -> str:
        return RootModel[MigrationConfig](self).model_dump_json(indent=4)
