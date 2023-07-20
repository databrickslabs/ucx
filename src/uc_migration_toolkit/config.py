from pydantic import Field
from pydantic.dataclasses import dataclass


@dataclass
class InventoryTable:
    catalog: str
    database: str
    name: str

    def __repr__(self):
        return f"{self.catalog}.{self.database}.{self.name}"

    def to_spark(self):
        return f"{self.catalog}.{self.database}.{self.name}"


@dataclass
class GroupsConfig:
    backup_group_prefix: str | None = "db-temp-"
    groups: list[str] | None = None
    auto: bool | None = True


@dataclass
class WorkspaceAuthConfig:
    token: str | None
    host: str | None


@dataclass
class AuthConfig:
    workspace: WorkspaceAuthConfig | None = None

    class Config:
        frozen = True


@dataclass
class InventoryConfig:
    table: InventoryTable


@dataclass
class RateLimitConfig:
    num_threads: int | None = 4
    max_requests_per_period: int | None = 100
    period_in_seconds: int | None = 1


@dataclass
class MigrationConfig:
    inventory: InventoryConfig
    with_table_acls: bool
    groups: GroupsConfig
    auth: AuthConfig | None = None
    rate_limit: RateLimitConfig | None = Field(default_factory=lambda: RateLimitConfig())
