from dataclasses import dataclass
from typing import List, Optional


@dataclass
class InventoryTable:
    catalog: str
    database: str
    table: str

    def __repr__(self):
        return f"{self.catalog}.{self.database}.{self.table}"


@dataclass
class GroupListingConfig:
    groups: Optional[List[str]] = None
    auto: Optional[bool] = True


@dataclass
class WorkspaceAuthConfig:
    token: Optional[str]
    host: Optional[str]


@dataclass
class AccountAuthConfig:
    account_id: str
    host: str
    password: str
    username: str


@dataclass
class AuthConfig:
    account: AccountAuthConfig
    workspace: Optional[WorkspaceAuthConfig] = None


@dataclass
class MigrationConfig:
    inventory_table: InventoryTable
    with_table_acls: bool
    auth_config: AuthConfig
    group_listing_config: GroupListingConfig
    backup_group_prefix: Optional[str] = "db-temp-"
