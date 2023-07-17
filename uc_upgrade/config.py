from dataclasses import dataclass
from typing import List, Optional


@dataclass
class InventoryTableName:
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
    host: str
    password: str
    username: str


@dataclass
class AuthConfig:
    account: AccountAuthConfig
    workspace: Optional[WorkspaceAuthConfig] = None


@dataclass
class MigrationConfig:
    inventory_table_name: InventoryTableName
    migrate_table_acls: bool
    auth_config: AuthConfig
    group_listing_config: GroupListingConfig
    num_threads: Optional[int] = 16
