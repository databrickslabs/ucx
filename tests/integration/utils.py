import logging
import random
from dataclasses import dataclass
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode
from databricks.sdk.service.iam import AccessControlRequest, Group, PermissionLevel
from databricks.sdk.service.jobs import JobCluster, PythonWheelTask, Task
from databricks.sdk.service.workspace import ObjectInfo

from databricks.labs.ucx.inventory.types import RequestObjectType

logger = logging.getLogger(__name__)


@dataclass
class InstanceProfile:
    instance_profile_arn: str
    iam_role_arn: str


@dataclass
class EnvironmentInfo:
    test_uid: str
    groups: list[tuple[Group, Group]]


def _set_random_permissions(
    objects: list[Any],
    id_attribute: str,
    request_object_type: RequestObjectType,
    env: EnvironmentInfo,
    ws: WorkspaceClient,
    permission_levels: list[PermissionLevel],
    num_acls: int | None = 3,
):
    def get_random_ws_group() -> Group:
        return random.choice([g[0] for g in env.groups])

    def get_random_permission_level() -> PermissionLevel:
        return random.choice(permission_levels)

    for _object in objects:
        acl_req = [
            AccessControlRequest(
                group_name=get_random_ws_group().display_name, permission_level=get_random_permission_level()
            )
            for _ in range(num_acls)
        ]

        ws.permissions.update(
            request_object_type=request_object_type,
            request_object_id=getattr(_object, id_attribute),
            access_control_list=acl_req,
        )


def _get_basic_job_cluster() -> JobCluster:
    return JobCluster(
        job_cluster_key="default",
        new_cluster=ClusterSpec(
            spark_version="13.2.x-scala2.12",
            node_type_id="i3.xlarge",
            driver_node_type_id="i3.xlarge",
            num_workers=0,
            spark_conf={"spark.master": "local[*, 4]", "spark.databricks.cluster.profile": "singleNode"},
            custom_tags={
                "ResourceClass": "SingleNode",
            },
            data_security_mode=DataSecurityMode.SINGLE_USER,
        ),
    )


def _get_basic_task() -> Task:
    return Task(
        task_key="test",
        python_wheel_task=PythonWheelTask(entry_point="main", package_name="some-pkg"),
        job_cluster_key="default",
    )


@dataclass
class WorkspaceObjects:
    root_dir: ObjectInfo
    notebooks: list[ObjectInfo]
    directories: list[ObjectInfo]
    # files: list[ObjectInfo]
    # repos: list[ObjectInfo]
