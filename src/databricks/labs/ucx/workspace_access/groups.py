import functools
import json
import logging
import re
from abc import abstractmethod
from collections.abc import Iterable, Collection
from dataclasses import dataclass
from datetime import timedelta
from typing import ClassVar

from databricks.labs.blueprint.limiter import rate_limited
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import (
    BadRequest,
    DeadlineExceeded,
    InternalError,
    NotFound,
    ResourceConflict,
)
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.iam import Group

from databricks.labs.ucx.framework.crawlers import CrawlerBase

logger = logging.getLogger(__name__)


@dataclass
class MigratedGroup:
    id_in_workspace: str
    name_in_workspace: str
    name_in_account: str
    temporary_name: str
    members: str | None = None
    entitlements: str | None = None
    external_id: str | None = None
    roles: str | None = None

    @classmethod
    def partial_info(cls, workspace: iam.Group, account: iam.Group):
        """This method is only intended for use in tests"""
        assert workspace.id is not None
        assert workspace.display_name is not None
        assert account.display_name is not None
        return cls(
            id_in_workspace=workspace.id,
            name_in_workspace=workspace.display_name,
            name_in_account=account.display_name,
            temporary_name=f"tmp-{workspace.display_name}",
            external_id=workspace.external_id,
        )

    def decode_members(self):
        return [iam.ComplexValue.from_dict(_) for _ in json.loads(self.members)]


class MigrationState:
    """Holds migration state of workspace-to-account groups"""

    def __init__(self, groups: list[MigratedGroup]):
        self._name_to_group: dict[str, MigratedGroup] = {_.name_in_workspace: _ for _ in groups}
        self._id_to_group: dict[str, MigratedGroup] = {_.id_in_workspace: _ for _ in groups}
        self.groups: list[MigratedGroup] = groups

    def get_target_principal(self, name: str) -> str | None:
        migrated_group = self._name_to_group.get(name)
        if migrated_group is None:
            return None
        return migrated_group.name_in_account

    def get_temp_principal(self, name: str) -> str | None:
        migrated_group = self._name_to_group.get(name)
        if migrated_group is None:
            return None
        return migrated_group.temporary_name

    def is_in_scope(self, name: str) -> bool:
        if name is None:
            return False
        return name in self._name_to_group

    def __len__(self):
        return len(self._name_to_group)

    def apply_to_renamed_groups(self, ws: WorkspaceClient) -> bool:
        """(Production) Apply migration state to groups that have been renamed in the workspace."""
        return self._apply_to_groups(ws, renamed=True)

    def apply_to_groups_with_different_names(self, ws: WorkspaceClient) -> bool:
        """(Integration Testing) Apply to groups that have different names in the workspace and account."""
        return self._apply_to_groups(ws, renamed=False)

    def _apply_to_groups(self, ws: WorkspaceClient, *, renamed: bool = False) -> bool:
        if len(self) == 0:
            logger.info("No valid groups selected, nothing to do.")
            return True
        logger.info(f"Migrating permissions for {len(self)} account groups.")
        total_permissions = 0
        success_groups = 0
        errors: list[Exception] = []
        for migrated_group in self.groups:
            name_in_workspace = migrated_group.name_in_workspace
            if renamed:
                # during integration testing, local and account group have completely different names,
                # that simplifies visual debuggability. In production, we need to first rename local
                # groups, otherwise we get `Workspace group XXX is not a workspace local group` and
                # the migration fails.
                name_in_workspace = migrated_group.temporary_name
            name_in_account = migrated_group.name_in_account
            try:
                group_permissions = self._migrate_group_permissions_paginated(ws, name_in_workspace, name_in_account)
                logger.info(
                    f"Migrated {group_permissions} permissions: {name_in_workspace} (workspace) -> {name_in_account} (account)"
                )
                total_permissions += group_permissions
                success_groups += 1
            except IOError as e:
                logger.error(f"failed-group-migration: {name_in_workspace} -> {name_in_account}: {e}")
                errors.append(e)
        logger.info(f"Migrated {total_permissions} permissions for {success_groups}/{len(self)} groups successfully.")
        if errors:
            logger.error(f"Migrating permissions failed for {len(errors)}/{len(self)} groups.")
            raise ManyError(errors)
        return True

    @staticmethod
    def _migrate_group_permissions_paginated(ws: WorkspaceClient, name_in_workspace: str, name_in_account: str) -> int:
        batch_size = 1000
        logger.info(f"Migrating permissions: {name_in_workspace} (workspace) -> {name_in_account} (account) starting")
        permissions_migrated = 0
        while True:
            result = ws.permission_migration.migrate_permissions(
                ws.get_workspace_id(),
                name_in_workspace,
                name_in_account,
                size=batch_size,
            )
            if not result.permissions_migrated:
                logger.info(
                    f"Migrating permissions: {name_in_workspace} (workspace) -> {name_in_account} (account) finished"
                )
                return permissions_migrated
            permissions_migrated += result.permissions_migrated
            logger.info(
                f"Migrating permissions: {name_in_workspace} (workspace) -> {name_in_account} (account) progress={permissions_migrated}(+{result.permissions_migrated})"
            )


class GroupMigrationStrategy:
    def __init__(
        self,
        workspace_groups_in_workspace,
        account_groups_in_account,
        /,
        renamed_groups_prefix,
        include_group_names=None,
    ):
        self.renamed_groups_prefix = renamed_groups_prefix
        self.workspace_groups_in_workspace = workspace_groups_in_workspace
        self.account_groups_in_account = account_groups_in_account
        self.include_group_names = include_group_names

    @abstractmethod
    def generate_migrated_groups(self) -> Iterable[MigratedGroup]:
        raise NotImplementedError

    def get_filtered_groups(self):
        if not self.include_group_names:
            logger.info("No group listing provided, all matching groups will be migrated")
            return self.workspace_groups_in_workspace
        logger.info("Group listing provided, a subset of all groups will be migrated")
        return {
            group_name: self.workspace_groups_in_workspace[group_name]
            for group_name in self.workspace_groups_in_workspace.keys()
            if group_name in self.include_group_names
        }

    @staticmethod
    def _safe_match(group_name: str, pattern: re.Pattern) -> str:
        try:
            match = pattern.search(group_name)
            if not match:
                return group_name
            match_groups = match.groups()
            if match_groups:
                return match_groups[0]
            return match.group()
        except re.error:
            return group_name

    @staticmethod
    def _safe_sub(group_name: str, pattern: re.Pattern, replace: str) -> str:
        try:
            return pattern.sub(replace, group_name)
        except re.error:
            logger.warning(f"Failed to apply Regex Expression {pattern} on Group Name {group_name}")
            return group_name


class MatchingNamesStrategy(GroupMigrationStrategy):
    def __init__(
        self,
        workspace_groups_in_workspace,
        account_groups_in_account,
        *,
        renamed_groups_prefix: str,
        include_group_names: list[str] | None,
    ):
        super().__init__(
            workspace_groups_in_workspace,
            account_groups_in_account,
            include_group_names=include_group_names,
            renamed_groups_prefix=renamed_groups_prefix,
        )

    def generate_migrated_groups(self) -> Iterable[MigratedGroup]:
        workspace_groups = self.get_filtered_groups()
        for group in workspace_groups.values():
            account_group = self.account_groups_in_account.get(group.display_name)
            if not account_group:
                logger.info(
                    f"Couldn't find a matching account group for {group.display_name} group using name matching"
                )
                continue
            temporary_name = f"{self.renamed_groups_prefix}{group.display_name}"
            yield MigratedGroup(
                id_in_workspace=group.id,
                name_in_workspace=group.display_name,
                name_in_account=group.display_name,
                temporary_name=temporary_name,
                external_id=account_group.external_id,
                members=json.dumps([gg.as_dict() for gg in group.members]) if group.members else None,
                roles=json.dumps([gg.as_dict() for gg in group.roles]) if group.roles else None,
                entitlements=json.dumps([gg.as_dict() for gg in group.entitlements]) if group.entitlements else None,
            )


class MatchByExternalIdStrategy(GroupMigrationStrategy):
    def __init__(
        self,
        workspace_groups_in_workspace,
        account_groups_in_account,
        *,
        renamed_groups_prefix: str,
        include_group_names: list[str] | None,
    ):
        super().__init__(
            workspace_groups_in_workspace,
            account_groups_in_account,
            include_group_names=include_group_names,
            renamed_groups_prefix=renamed_groups_prefix,
        )

    def generate_migrated_groups(self) -> Iterable[MigratedGroup]:
        workspace_groups = self.get_filtered_groups()
        account_groups_by_id = {group.external_id: group for group in self.account_groups_in_account.values()}
        for group in workspace_groups.values():
            account_group = account_groups_by_id.get(group.external_id)
            if not account_group:
                logger.info(f"Couldn't find a matching account group for {group.display_name} group with external_id")
                continue
            temporary_name = f"{self.renamed_groups_prefix}{group.display_name}"
            yield MigratedGroup(
                id_in_workspace=group.id,
                name_in_workspace=group.display_name,
                name_in_account=account_group.display_name,
                temporary_name=temporary_name,
                external_id=account_group.external_id,
                members=json.dumps([gg.as_dict() for gg in group.members]) if group.members else None,
                roles=json.dumps([gg.as_dict() for gg in group.roles]) if group.roles else None,
                entitlements=(json.dumps([gg.as_dict() for gg in group.entitlements]) if group.entitlements else None),
            )


class RegexSubStrategy(GroupMigrationStrategy):
    def __init__(
        self,
        workspace_groups_in_workspace: dict[str, Group],
        account_groups_in_account: dict[str, Group],
        *,
        renamed_groups_prefix: str,
        include_group_names: list[str] | None,
        workspace_group_regex: str,
        workspace_group_replace: str,
    ):
        super().__init__(
            workspace_groups_in_workspace,
            account_groups_in_account,
            include_group_names=include_group_names,
            renamed_groups_prefix=renamed_groups_prefix,
        )
        self.workspace_group_regex = workspace_group_regex  # Keep to support legacy public API
        self.workspace_group_replace = workspace_group_replace

        self._workspace_group_pattern = re.compile(self.workspace_group_regex)

    def generate_migrated_groups(self) -> Iterable[MigratedGroup]:
        workspace_groups = self.get_filtered_groups()
        for group in workspace_groups.values():
            name_in_account = self._safe_sub(
                group.display_name,
                self._workspace_group_pattern,
                self.workspace_group_replace,
            )
            account_group = self.account_groups_in_account.get(name_in_account)
            if not account_group:
                logger.info(
                    f"Couldn't find a matching account group for {group.display_name} group with regex substitution"
                )
                continue
            temporary_name = f"{self.renamed_groups_prefix}{group.display_name}"
            yield MigratedGroup(
                id_in_workspace=group.id,
                name_in_workspace=group.display_name,
                name_in_account=name_in_account,
                temporary_name=temporary_name,
                external_id=account_group.external_id,
                members=json.dumps([gg.as_dict() for gg in group.members]) if group.members else None,
                roles=json.dumps([gg.as_dict() for gg in group.roles]) if group.roles else None,
                entitlements=json.dumps([gg.as_dict() for gg in group.entitlements]) if group.entitlements else None,
            )


class RegexMatchStrategy(GroupMigrationStrategy):
    def __init__(
        self,
        workspace_groups_in_workspace,
        account_groups_in_account,
        *,
        renamed_groups_prefix: str,
        include_group_names: list[str] | None,
        workspace_group_regex: str,
        account_group_regex: str,
    ):
        super().__init__(
            workspace_groups_in_workspace,
            account_groups_in_account,
            include_group_names=include_group_names,
            renamed_groups_prefix=renamed_groups_prefix,
        )
        # Keep to support legacy public API
        self.workspace_group_regex = workspace_group_regex
        self.account_group_regex = account_group_regex

        self._workspace_group_pattern = re.compile(self.workspace_group_regex)
        self._account_group_pattern = re.compile(self.account_group_regex)

    def generate_migrated_groups(self) -> Iterable[MigratedGroup]:
        workspace_groups_by_match = {
            self._safe_match(group_name, self._workspace_group_pattern): group
            for group_name, group in self.get_filtered_groups().items()
        }
        account_groups_by_match = {
            self._safe_match(group_name, self._account_group_pattern): group
            for group_name, group in self.account_groups_in_account.items()
        }
        for group_match, ws_group in workspace_groups_by_match.items():
            account_group = account_groups_by_match.get(group_match)
            if not account_group:
                logger.info(
                    f"Couldn't find a matching account group for {ws_group.display_name} group with regex matching"
                )
                continue
            temporary_name = f"{self.renamed_groups_prefix}{ws_group.display_name}"
            yield MigratedGroup(
                id_in_workspace=ws_group.id,
                name_in_workspace=ws_group.display_name,
                name_in_account=account_group.display_name,
                temporary_name=temporary_name,
                external_id=account_group.external_id,
                members=json.dumps([gg.as_dict() for gg in ws_group.members]) if ws_group.members else None,
                roles=json.dumps([gg.as_dict() for gg in ws_group.roles]) if ws_group.roles else None,
                entitlements=(
                    json.dumps([gg.as_dict() for gg in ws_group.entitlements]) if ws_group.entitlements else None
                ),
            )


class GroupRenameIncompleteError(RuntimeError):
    __slots__ = ("group_id", "old_name", "new_name")

    def __init__(self, group_id: str, old_name: str, new_name: str) -> None:
        super().__init__(f"Rename incomplete for group {group_id}: {old_name} -> {new_name}")
        self.group_id = group_id
        self.old_name = old_name
        self.new_name = new_name


class GroupManager(CrawlerBase[MigratedGroup]):
    _SYSTEM_GROUPS: ClassVar[list[str]] = ["users", "admins", "account users"]

    def __init__(  # pylint: disable=too-many-arguments
        self,
        sql_backend: SqlBackend,
        ws: WorkspaceClient,
        inventory_database: str,
        include_group_names: list[str] | None = None,
        renamed_group_prefix: str | None = "ucx-renamed-",
        workspace_group_regex: str | None = None,
        workspace_group_replace: str | None = None,
        account_group_regex: str | None = None,
        verify_timeout: timedelta | None = timedelta(minutes=2),
        *,
        external_id_match: bool = False,
    ):
        super().__init__(sql_backend, "hive_metastore", inventory_database, "groups", MigratedGroup)
        if not renamed_group_prefix:
            renamed_group_prefix = "ucx-renamed-"

        self._ws = ws
        self._include_group_names = include_group_names
        self._renamed_group_prefix = renamed_group_prefix
        self._workspace_group_regex = workspace_group_regex
        self._workspace_group_replace = workspace_group_replace
        self._account_group_regex = account_group_regex
        self._external_id_match = external_id_match
        self._verify_timeout = verify_timeout

    def snapshot(self) -> list[MigratedGroup]:
        return self._snapshot(self._fetcher, self._crawler)

    def has_groups(self) -> bool:
        return len(self.snapshot()) > 0

    def rename_groups(self):
        tasks = []
        account_groups_in_workspace = self._account_groups_in_workspace()
        workspace_groups_in_workspace = self._workspace_groups_in_workspace()
        groups_to_migrate = self.get_migration_state().groups

        for migrated_group in groups_to_migrate:
            if migrated_group.name_in_account in account_groups_in_workspace:
                logger.info(f"Skipping {migrated_group.name_in_account}: already in workspace")
                continue
            if migrated_group.temporary_name in workspace_groups_in_workspace:
                logger.info(f"Skipping {migrated_group.name_in_workspace}: already renamed")
                continue
            logger.info(f"Renaming: {migrated_group.name_in_workspace} -> {migrated_group.temporary_name}")
            tasks.append(
                functools.partial(
                    self._rename_group_and_wait_for_rename,
                    migrated_group.id_in_workspace,
                    migrated_group.name_in_workspace,
                    migrated_group.temporary_name,
                )
            )
        renamed_groups = Threads.strict("rename groups in the workspace", tasks)
        # Renaming is eventually consistent, and the tasks above have each polled to verify their rename completed.
        # Here we also check that enumeration yields the updated names; this is necessary because otherwise downstream
        # tasks (like reflect_account_groups_on_workspace()) may skip a renamed group because it doesn't appear to be
        # present.
        self._wait_for_renamed_groups(renamed_groups)

    def _rename_group_and_wait_for_rename(self, group_id: str, old_group_name, new_group_name: str) -> tuple[str, str]:
        logger.debug(f"Renaming group {group_id}: {old_group_name} -> {new_group_name}")
        self._rename_group(group_id, new_group_name)
        logger.debug(f"Waiting for group {group_id} rename to take effect: {old_group_name} -> {new_group_name}")
        self._wait_for_rename(group_id, old_group_name, new_group_name)
        return group_id, new_group_name

    @retried(on=[InternalError, ResourceConflict, DeadlineExceeded])
    @rate_limited(max_requests=10, burst_period_seconds=60)
    def _rename_group(self, group_id: str, new_group_name: str) -> None:
        ops = [iam.Patch(iam.PatchOp.REPLACE, "displayName", new_group_name)]
        self._ws.groups.patch(group_id, operations=ops)

    @retried(on=[GroupRenameIncompleteError], timeout=timedelta(minutes=2))
    def _wait_for_rename(self, group_id: str, old_group_name: str, new_group_name: str) -> None:
        group = self._ws.groups.get(group_id)
        if group.display_name == old_group_name:
            # Rename still pending.
            raise GroupRenameIncompleteError(group_id, old_group_name, new_group_name)
        if group.display_name != new_group_name:
            # Group has an entirely unexpected name; something else is interfering.
            msg = f"While waiting for group {group_id} rename ({old_group_name} -> {new_group_name} an unexpected name was observed: {group.display_name}"
            raise RuntimeError(msg)
        # Normal exit; group has been renamed.

    @retried(on=[ManyError], timeout=timedelta(minutes=2))
    def _wait_for_renamed_groups(self, expected_groups: Collection[tuple[str, str]]) -> None:
        attributes = "id,displayName"
        found_groups = {
            group.id: group.display_name
            for group in self._list_workspace_groups("WorkspaceGroup", attributes)
            if group.display_name
        }
        pending_renames: list[RuntimeError] = []
        for group_id, expected_name in expected_groups:
            found_name = found_groups.get(group_id, None)
            if found_name is None:
                pending_renames.append(RuntimeError(f"Missing group with id: {group_id} (renamed to {expected_name}"))
            elif found_name != expected_name:
                pending_renames.append(GroupRenameIncompleteError(group_id, found_name, expected_name))
        if pending_renames:
            raise ManyError(pending_renames)

    def reflect_account_groups_on_workspace(self):
        tasks = []
        account_groups_in_account = self._account_groups_in_account()
        account_groups_in_workspace = self._account_groups_in_workspace()
        groups_to_migrate = self.get_migration_state().groups
        for migrated_group in groups_to_migrate:
            if migrated_group.name_in_account in account_groups_in_workspace:
                logger.info(f"Skipping {migrated_group.name_in_account}: already in workspace")
                continue
            if migrated_group.name_in_account not in account_groups_in_account:
                logger.warning(f"Skipping {migrated_group.name_in_account}: not in account")
                continue
            group_id = account_groups_in_account[migrated_group.name_in_account].id
            tasks.append(functools.partial(self._reflect_account_group_to_workspace, group_id))
        _, errors = Threads.gather("reflect account groups on this workspace", tasks)
        if len(errors) > 0:
            raise ManyError(errors)

    def get_migration_state(self) -> MigrationState:
        return MigrationState(self.snapshot())

    def delete_original_workspace_groups(self):
        tasks = []
        workspace_groups_in_workspace = self._workspace_groups_in_workspace()
        account_groups_in_workspace = self._account_groups_in_workspace()
        for migrated_group in self.snapshot():
            if migrated_group.temporary_name not in workspace_groups_in_workspace:
                logger.info(f"Skipping {migrated_group.name_in_workspace}: no longer in workspace")
                continue
            if migrated_group.name_in_account not in account_groups_in_workspace:
                logger.info(f"Skipping {migrated_group.name_in_account}: not reflected in workspace")
                continue
            tasks.append(
                functools.partial(
                    self._delete_workspace_group, migrated_group.id_in_workspace, migrated_group.temporary_name
                )
            )
        _, errors = Threads.gather("removing original workspace groups", tasks)
        if len(errors) > 0:
            logger.error(f"During account-to-workspace reflection got {len(errors)} errors. See debug logs")
            raise ManyError(errors)

    def _fetcher(self) -> Iterable[MigratedGroup]:
        state = []
        for row in self._backend.fetch(f"SELECT * FROM {self.full_name}"):
            state.append(MigratedGroup(*row))

        if not self._include_group_names:
            return state

        new_state = []
        group_name_with_state = {migrated_group.name_in_workspace: migrated_group for migrated_group in state}
        for group_name in self._include_group_names:
            if group_name in group_name_with_state:
                new_state.append(group_name_with_state[group_name])
            else:
                logger.warning(
                    f"Group {group_name} defined in configuration does not exist on the groups table. "
                    "Consider checking if the group exist in the workspace or re-running the assessment."
                )
        return new_state

    def _crawler(self) -> Iterable[MigratedGroup]:
        workspace_groups_in_workspace = self._workspace_groups_in_workspace()
        account_groups_in_account = self._account_groups_in_account()
        strategy = self._get_strategy(workspace_groups_in_workspace, account_groups_in_account)
        yield from strategy.generate_migrated_groups()

    def validate_group_membership(self) -> list[dict]:
        workspace_groups_in_workspace = self._workspace_groups_in_workspace()
        account_groups_in_account = self._account_groups_in_account()
        strategy = self._get_strategy(workspace_groups_in_workspace, account_groups_in_account)
        migrated_groups = strategy.generate_migrated_groups()
        mismatch_group = []
        retry_on_internal_error = retried(on=[InternalError], timeout=self._verify_timeout)
        get_account_group = retry_on_internal_error(self._get_account_group)
        for ws_group in migrated_groups:
            # Users with the same display name but different email will be deduplicated!
            ws_members_set = {m.get("display") for m in json.loads(ws_group.members)} if ws_group.members else set()
            acc_group = get_account_group(account_groups_in_account[ws_group.name_in_account].id)
            if not acc_group:
                continue  # group not present anymore
            acc_members_set = {a.as_dict().get("display") for a in acc_group.members} if acc_group.members else set()
            set_diff = (ws_members_set - acc_members_set).union(acc_members_set - ws_members_set)
            if not set_diff:
                continue
            mismatch_group.append(
                {
                    "wf_group_name": ws_group.name_in_workspace,
                    "wf_group_members_count": len(ws_members_set),
                    "acc_group_name": ws_group.name_in_account,
                    "acc_group_members_count": len(acc_members_set),
                    "group_members_difference": len(ws_members_set) - len(acc_members_set),
                }
            )
        if not mismatch_group:
            logger.info("There are no groups with different membership between account and workspace")
        else:
            logger.info("There are groups with different membership between account and workspace")
        return mismatch_group

    def has_workspace_group(self, name):
        groups = self._workspace_groups_in_workspace()
        return name in groups

    def has_account_group(self, name):
        groups = self._account_groups_in_workspace()
        return name in groups

    def _workspace_groups_in_workspace(self) -> dict[str, Group]:
        attributes = "id,displayName,meta,externalId,members,roles,entitlements"
        groups = {}
        for group in self._list_workspace_groups("WorkspaceGroup", attributes):
            if not group.display_name:
                continue
            groups[group.display_name] = group
        return groups

    def _account_groups_in_workspace(self) -> dict[str, Group]:
        groups = {}
        for group in self._list_workspace_groups("Group", "id,displayName,externalId,meta"):
            if not group.display_name:
                continue
            groups[group.display_name] = group
        return groups

    def _account_groups_in_account(self) -> dict[str, Group]:
        groups = {}
        for group in self._list_account_groups("id,displayName,externalId"):
            if not group.display_name:
                continue
            groups[group.display_name] = group
        return groups

    def _is_group_out_of_scope(self, group: iam.Group, resource_type: str) -> bool:
        if group.display_name in self._SYSTEM_GROUPS:
            return True
        meta = group.meta
        if not meta:
            return False
        if meta.resource_type != resource_type:
            return True
        return False

    def _list_workspace_groups(self, resource_type: str, scim_attributes: str) -> list[iam.Group]:
        results = []
        logger.info(f"Listing workspace groups (resource_type={resource_type}) with {scim_attributes}...")
        # these attributes can get too large causing the api to timeout
        # so we're fetching groups without these attributes first
        # and then calling get on each of them to fetch all attributes
        attributes = scim_attributes.split(",")
        if "members" in attributes:
            attributes.remove("members")
            retry_on_internal_error = retried(on=[InternalError], timeout=self._verify_timeout)
            get_group = retry_on_internal_error(self._get_group)
            for group in self._ws.groups.list(attributes=",".join(attributes)):
                if self._is_group_out_of_scope(group, resource_type):
                    continue
                group_with_all_attributes = get_group(group.id)
                if not group_with_all_attributes:
                    continue
                results.append(group_with_all_attributes)
        else:
            for group in self._ws.groups.list(attributes=scim_attributes):
                if self._is_group_out_of_scope(group, resource_type):
                    continue
                results.append(group)
        logger.info(f"Found {len(results)} {resource_type}")
        return results

    @rate_limited(max_requests=255, burst_period_seconds=60)
    def _get_group(self, group_id: str) -> iam.Group | None:
        try:
            return self._ws.groups.get(group_id)
        except NotFound:
            # during integration tests, we may get certain groups removed,
            # which will cause timeout errors because of groups no longer there.
            return None

    @rate_limited(max_requests=20)
    def _get_account_group(self, group_id: str) -> Group | None:
        try:
            raw = self._ws.api_client.do("GET", f"/api/2.0/account/scim/v2/Groups/{group_id}")
            return iam.Group.from_dict(raw)  # type: ignore[arg-type]
        except NotFound:
            # the given group has been removed from the account after getting the group and before running this method
            logger.warning(f"Group with ID {group_id} does not exist anymore in the Databricks account.")
            return None

    def _list_account_groups(self, scim_attributes: str) -> list[iam.Group]:
        # TODO: we should avoid using this method, as it's not documented
        # get account-level groups even if they're not (yet) assigned to a workspace
        logger.info(f"Listing account groups with {scim_attributes}...")
        account_groups = []
        raw = self._ws.api_client.do("GET", "/api/2.0/account/scim/v2/Groups", query={"attributes": scim_attributes})
        for resource in raw.get("Resources", []):  # type: ignore[union-attr]
            group = iam.Group.from_dict(resource)
            if group.display_name in self._SYSTEM_GROUPS:
                continue
            account_groups.append(group)
        logger.info(f"Found {len(account_groups)} account groups")
        sorted_groups: list[iam.Group] = sorted(account_groups, key=lambda _: _.display_name)  # type: ignore[arg-type,return-value]
        return sorted_groups

    @retried(on=[InternalError, ResourceConflict, DeadlineExceeded])
    @rate_limited(max_requests=35, burst_period_seconds=60)
    def _delete_workspace_group(self, group_id: str, display_name: str) -> None:
        try:
            logger.info(f"Deleting the workspace-level group {display_name} with id {group_id}")
            self._ws.groups.delete(id=group_id)
            logger.info(f"Workspace-level group {display_name} with id {group_id} was deleted")
            return None
        except NotFound:
            return None

    @retried(on=[InternalError, ResourceConflict, DeadlineExceeded])
    @rate_limited(max_requests=5)
    def _reflect_account_group_to_workspace(self, account_group_id: str):
        try:
            # TODO: add OpenAPI spec for it
            path = f"/api/2.0/preview/permissionassignments/principals/{account_group_id}"
            self._ws.api_client.do("PUT", path, data=json.dumps({"permissions": ["USER"]}))
            return True
        except BadRequest:
            # already exists
            return True
        except NotFound:
            # the given group has been removed from the account after getting the group and before running this method
            logger.warning(f"Group with ID {account_group_id} does not exist anymore in the Databricks account.")
            return True

    def _get_strategy(
        self, workspace_groups_in_workspace: dict[str, Group], account_groups_in_account: dict[str, Group]
    ) -> GroupMigrationStrategy:
        if self._workspace_group_regex is not None and self._workspace_group_replace is not None:
            return RegexSubStrategy(
                workspace_groups_in_workspace,
                account_groups_in_account,
                renamed_groups_prefix=self._renamed_group_prefix,
                include_group_names=self._include_group_names,
                workspace_group_regex=self._workspace_group_regex,
                workspace_group_replace=self._workspace_group_replace,
            )
        if self._workspace_group_regex is not None and self._account_group_regex is not None:
            return RegexMatchStrategy(
                workspace_groups_in_workspace,
                account_groups_in_account,
                renamed_groups_prefix=self._renamed_group_prefix,
                include_group_names=self._include_group_names,
                workspace_group_regex=self._workspace_group_regex,
                account_group_regex=self._account_group_regex,
            )
        if self._external_id_match:
            return MatchByExternalIdStrategy(
                workspace_groups_in_workspace,
                account_groups_in_account,
                renamed_groups_prefix=self._renamed_group_prefix,
                include_group_names=self._include_group_names,
            )
        return MatchingNamesStrategy(
            workspace_groups_in_workspace,
            account_groups_in_account,
            renamed_groups_prefix=self._renamed_group_prefix,
            include_group_names=self._include_group_names,
        )


class ConfigureGroups:
    renamed_group_prefix = "db-temp-"
    workspace_group_regex = None
    workspace_group_replace = None
    account_group_regex = None
    group_match_by_external_id = None
    include_group_names = None

    _valid_substitute_pattern = re.compile(r"[\s#,+ \\<>;]")

    def __init__(self, prompts: Prompts):
        self._prompts = prompts
        self._ask_for_group = functools.partial(self._prompts.question, validate=self._is_valid_group_str)
        self._ask_for_substitute = functools.partial(self._prompts.question, validate=self._is_valid_substitute_str)
        self._ask_for_regex = functools.partial(self._prompts.question, validate=self._validate_regex)

    def run(self):
        self.renamed_group_prefix = self._ask_for_group("Backup prefix", default=self.renamed_group_prefix)
        strategy = self._prompts.choice_from_dict(
            "Choose how to map the workspace groups:",
            {
                "Match by Name": lambda: True,
                "Apply a Prefix": self._configure_prefix,
                "Apply a Suffix": self._configure_suffix,
                "Match by External ID": self._configure_external,
                "Regex Substitution": self._configure_substitution,
                "Regex Matching": self._configure_matching,
            },
            sort=False,
        )
        strategy()
        self._configure_names()

    def _configure_prefix(self):
        prefix = self._ask_for_group("Enter a prefix to add to the workspace group name")
        if not prefix:
            return False
        self.workspace_group_regex = "^"
        self.workspace_group_replace = prefix
        return True

    def _configure_suffix(self):
        suffix = self._ask_for_group("Enter a suffix to add to the workspace group name")
        if not suffix:
            return False
        self.workspace_group_regex = "$"
        self.workspace_group_replace = suffix
        return True

    def _configure_substitution(self):
        match_value = self._ask_for_regex("Enter a regular expression for substitution")
        if not match_value:
            return False
        substitute = self._ask_for_substitute("Enter the substitution value")
        if substitute is None:
            return False
        self.workspace_group_regex = match_value
        self.workspace_group_replace = substitute
        return True

    def _configure_matching(self):
        ws_match_value = self._ask_for_regex("Enter a regular expression to match on the workspace group")
        if not ws_match_value:
            return False
        acct_match_value = self._ask_for_regex("Enter a regular expression to match on the account group")
        if not acct_match_value:
            return False
        self.workspace_group_regex = ws_match_value
        self.account_group_regex = acct_match_value
        return True

    def _configure_names(self):
        selected_groups = self._prompts.question(
            "Comma-separated list of workspace group names to migrate. If not specified, we'll use all "
            "account-level groups with matching names to workspace-level groups",
            default="<ALL>",
        )
        if selected_groups != "<ALL>":
            self.include_group_names = [x.strip() for x in selected_groups.split(",")]
        return True

    def _configure_external(self):
        self.group_match_by_external_id = True
        return True

    def _is_valid_group_str(self, group_str: str) -> bool:
        return len(group_str) > 0 and self._is_valid_substitute_str(group_str)

    def _is_valid_substitute_str(self, substitute: str) -> bool:
        return not self._valid_substitute_pattern.search(substitute)

    @staticmethod
    def _validate_regex(regex_input: str) -> bool:
        try:
            re.compile(regex_input)
            return True
        except re.error:
            logger.error(f"{regex_input} is an invalid regular expression")
            return False
