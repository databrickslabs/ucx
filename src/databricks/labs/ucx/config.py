from dataclasses import dataclass
from pathlib import Path

from databricks.sdk.core import Config

from databricks.labs.ucx.__about__ import __version__


@dataclass
class InventoryTable:
    catalog: str
    database: str
    name: str

    def __repr__(self):
        return f"{self.catalog}.{self.database}.{self.name}"

    def to_spark(self):
        return self.__repr__()

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(**raw)


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
class InventoryConfig:
    table: InventoryTable

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(table=InventoryTable.from_dict(raw.get("table")))


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
        )

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(**raw)


@dataclass
class TaclConfig:
    databases: list[str]

    @classmethod
    def from_dict(cls, raw: dict):
        return cls(**raw)


@dataclass
class MigrationConfig:
    inventory: InventoryConfig
    groups: GroupsConfig
    tacl: TaclConfig
    connect: ConnectConfig | None = None
    num_threads: int | None = 4
    log_level: str | None = "INFO"

    def as_dict(self) -> dict:
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

        return inner(self)

    @classmethod
    def from_dict(cls, raw: dict) -> "MigrationConfig":
        return cls(
            inventory=InventoryConfig.from_dict(raw.get("inventory", {})),
            groups=GroupsConfig.from_dict(raw.get("groups", {})),
            tacl=TaclConfig.from_dict(raw.get("tacl", {})),
            connect=ConnectConfig.from_dict(raw.get("connect", {})),
            num_threads=raw.get("num_threads", 4),
            log_level=raw.get("log_level", "INFO"),
        )

    @classmethod
    def from_file(cls, config_file: Path) -> "MigrationConfig":
        from yaml import safe_load

        raw = safe_load(config_file.read_text())
        return MigrationConfig.from_dict({} if not raw else raw)

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
            product="ucx",
            product_version=__version__,
        )