import json
from dataclasses import asdict, dataclass
from typing import Literal

import pandas as pd

from databricks.labs.ucx.generic import StrEnum

Destination = Literal["backup", "account"]


class RequestObjectType(StrEnum):
    AUTHORIZATION = "authorization"  # tokens and passwords are here too!
    CLUSTERS = "clusters"
    CLUSTER_POLICIES = "cluster-policies"
    DIRECTORIES = "directories"
    EXPERIMENTS = "experiments"
    FILES = "files"
    INSTANCE_POOLS = "instance-pools"
    JOBS = "jobs"
    NOTEBOOKS = "notebooks"
    PIPELINES = "pipelines"
    REGISTERED_MODELS = "registered-models"
    REPOS = "repos"
    SQL_WAREHOUSES = "sql/warehouses"  # / is not a typo, it's the real object type

    def __repr__(self):
        return self.value


@dataclass
class PermissionsInventoryItem:
    object_id: str
    crawler: str  # shall be taken from CRAWLERS dict
    raw_object_permissions: str
    raw_extras: str | None = None  # any additional information should be stored here in a JSON-formatted dictionary

    def extras(self) -> dict:
        return json.loads(self.raw_extras) if self.raw_extras else {}

    @staticmethod
    def from_pandas(source: pd.DataFrame) -> list["PermissionsInventoryItem"]:
        items = source.to_dict(orient="records")
        return [PermissionsInventoryItem.from_dict(item) for item in items]

    def as_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict) -> "PermissionsInventoryItem":
        return cls(
            object_id=raw["object_id"],
            raw_object_permissions=raw["raw_object_permissions"],
            crawler=raw["crawler"],
        )
