import random
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.iam import (
    AccessControlRequest,
    ComplexValue,
    Group,
    PermissionLevel,
    User,
)
from dotenv import load_dotenv

from uc_migration_toolkit.managers.inventory.types import RequestObjectType
from uc_migration_toolkit.providers.client import ImprovedWorkspaceClient
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.utils import WorkspaceLevelEntitlement


def initialize_env() -> None:
    principal_env = Path(__file__).parent.parent.parent / ".env.principal"

    if principal_env.exists():
        logger.debug("Using credentials provided in .env.principal")
        load_dotenv(dotenv_path=principal_env)
    else:
        logger.debug(f"No .env.principal found at {principal_env.absolute()}, using environment variables")


@dataclass
class InstanceProfile:
    instance_profile_arn: str
    iam_role_arn: str


@dataclass
class EnvironmentInfo:
    test_uid: str
    groups: list[tuple[Group, Group]]


def generate_group_by_id(
    _ws: WorkspaceClient, _acc: AccountClient, group_name: str, users_sample: list[User]
) -> tuple[Group, Group]:
    entities = [ComplexValue(display=user.display_name, value=user.id) for user in users_sample]
    logger.debug(f"Creating group with name {group_name}")

    def get_random_entitlements():
        chosen: list[WorkspaceLevelEntitlement] = random.choices(
            list(WorkspaceLevelEntitlement),
            k=random.randint(1, 3),
        )
        entitlements = [ComplexValue(display=None, primary=None, type=None, value=value) for value in chosen]
        return entitlements

    ws_group = _ws.groups.create(display_name=group_name, members=entities, entitlements=get_random_entitlements())
    acc_group = _acc.groups.create(display_name=group_name, members=entities)
    return ws_group, acc_group


def _create_groups(
    _ws: ImprovedWorkspaceClient, _acc: AccountClient, prefix: str, num_test_groups: int, threader: callable
) -> list[tuple[Group, Group]]:
    logger.debug("Listing users to create sample groups")
    test_users = list(_ws.users.list(filter="displayName sw 'test-user-'", attributes="id, userName, displayName"))
    logger.debug(f"Total of test users {len(test_users)}")
    user_samples: dict[str, list[User]] = {
        f"{prefix}-test-group-{gid}": random.choices(test_users, k=random.randint(1, 40))
        for gid in range(num_test_groups)
    }
    executables = [
        partial(generate_group_by_id, _ws, _acc, group_name, users_sample)
        for group_name, users_sample in user_samples.items()
    ]
    return threader(executables).run()


def _cleanup_groups(_ws: WorkspaceClient, _acc: AccountClient, _groups: tuple[Group, Group]):
    ws_g, acc_g = _groups
    logger.debug(f"Deleting groups {ws_g.display_name} [ws-level] and {acc_g.display_name} [acc-level]")

    try:
        _ws.groups.delete(ws_g.id)
    except Exception as e:
        logger.warning(f"Cannot delete ws-level group {ws_g.display_name}, skipping it. Original exception {e}")

    try:
        g = next(iter(_acc.groups.list(filter=f"displayName eq '{acc_g.display_name}'")), None)
        if g:
            _acc.groups.delete(g.id)
    except Exception as e:
        logger.warning(f"Cannot delete acc-level group {acc_g.display_name}, skipping it. Original exception {e}")


def _set_random_permissions(
    objects: list[Any],
    id_attribute: str,
    request_object_type: RequestObjectType,
    env: EnvironmentInfo,
    ws: ImprovedWorkspaceClient,
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

        ws.permissions.set(
            request_object_type=request_object_type,
            request_object_id=getattr(_object, id_attribute),
            access_control_list=acl_req,
        )
