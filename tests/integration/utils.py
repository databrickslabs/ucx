import logging
from dataclasses import dataclass

from databricks.sdk.service.iam import Group
from databricks.sdk.service.workspace import ObjectInfo

logger = logging.getLogger(__name__)


@dataclass
class InstanceProfile:
    instance_profile_arn: str
    iam_role_arn: str


@dataclass
class EnvironmentInfo:
    test_uid: str
    groups: list[tuple[Group, Group]]


@dataclass
class WorkspaceObjects:
    root_dir: ObjectInfo
    notebooks: list[ObjectInfo]
    directories: list[ObjectInfo]
    # files: list[ObjectInfo]
    # repos: list[ObjectInfo]
