from dataclasses import dataclass
from typing import List, Optional


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
    cloud: str


@dataclass
class AuthConfig:
    workspace: Optional[WorkspaceAuthConfig]
    account: AccountAuthConfig


@dataclass
class MigrationConfig:
    inventory_table_name: str
    migrate_table_acls: bool
    auth_config: AuthConfig
    group_listing_config: GroupListingConfig
    num_threads: Optional[int] = 16
