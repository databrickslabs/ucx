"""
HIVE metastore objects represented by dataclasses.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Catalog:
    name: str

    @property
    def full_name(self) -> str:
        return self.name

    @property
    def key(self) -> str:
        return self.full_name

    @property
    def kind(self) -> str:
        return "CATALOG"


@dataclass(frozen=True)
class Schema:
    catalog: str
    name: str

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.name}"

    @property
    def key(self) -> str:
        return self.full_name

    @property
    def kind(self) -> str:
        return "DATABASE"
