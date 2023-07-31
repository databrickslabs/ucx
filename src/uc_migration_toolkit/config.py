from pydantic import RootModel
from pydantic.dataclasses import dataclass


@dataclass
class InventoryTable:
    catalog: str
    database: str
    name: str

    def __repr__(self):
        return f"{self.catalog}.{self.database}.{self.name}"

    def to_spark(self):
        return self.__repr__()


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
class WorkspaceAuthConfig:
    token: str | None = None
    host: str | None = None
    client_id: str | None = None
    client_secret: str | None = None


@dataclass
class AuthConfig:
    workspace: WorkspaceAuthConfig | None = None

    class Config:
        frozen = True


@dataclass
class InventoryConfig:
    table: InventoryTable


@dataclass
class MigrationConfig:
    inventory: InventoryConfig
    with_table_acls: bool
    groups: GroupsConfig
    auth: AuthConfig | None = None
    num_threads: int | None = 4

    def __post_init__(self):
        if self.with_table_acls:
            msg = "Table ACLS are not yet implemented"
            raise NotImplementedError(msg)

    def to_json(self) -> str:
        return RootModel[MigrationConfig](self).model_dump_json(indent=4)
