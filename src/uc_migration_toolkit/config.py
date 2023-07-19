from dataclasses import dataclass


@dataclass
class InventoryTable:
    catalog: str
    database: str
    table: str

    def __repr__(self):
        return f"{self.catalog}.{self.database}.{self.table}"


@dataclass
class GroupListingConfig:
    groups: list[str] | None = None
    auto: bool | None = True


@dataclass
class WorkspaceAuthConfig:
    token: str | None
    host: str | None


@dataclass
class AccountAuthConfig:
    account_id: str
    host: str
    password: str
    username: str


@dataclass
class AuthConfig:
    account: AccountAuthConfig
    workspace: WorkspaceAuthConfig | None = None


@dataclass
class MigrationConfig:
    inventory_table: InventoryTable
    with_table_acls: bool
    auth_config: AuthConfig
    group_listing_config: GroupListingConfig
    backup_group_prefix: str | None = "db-temp-"
