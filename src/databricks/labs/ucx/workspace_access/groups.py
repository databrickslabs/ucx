import functools
import json
import logging
import typing
from dataclasses import dataclass
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import InternalError
from databricks.sdk.retries import retried
from databricks.sdk.service import iam

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend
from databricks.labs.ucx.framework.parallel import Threads
from databricks.labs.ucx.mixins.hardening import rate_limited

logger = logging.getLogger(__name__)


@dataclass
class MigratedGroup:
    id_in_workspace: str
    name_in_workspace: str
    name_in_account: str
    temporary_name: str
    members: str = None
    entitlements: str = None
    external_id: str = None
    roles: str = None

    @classmethod
    def partial_info(cls, workspace: iam.Group, account: iam.Group):
        """This method is only intended for use in tests"""
        return cls(
            id_in_workspace=workspace.id,
            name_in_workspace=workspace.display_name,
            name_in_account=account.display_name,
            temporary_name=f"tmp-{workspace.display_name}",
            external_id=workspace.external_id,
        )


class MigrationState:
    """Holds migration state of workspace-to-account groups"""

    def __init__(self, groups: list[MigratedGroup]):
        self._name_to_group: dict[str, MigratedGroup] = {_.name_in_workspace: _ for _ in groups}
        self._id_to_group: dict[str, MigratedGroup] = {_.id_in_workspace: _ for _ in groups}
        self.groups: list[MigratedGroup] = groups

    def get_target_principal(self, name: str) -> str | None:
        mg = self._name_to_group.get(name)
        if mg is None:
            return None
        return mg.name_in_account

    def is_in_scope(self, name: str) -> bool:
        if name is None:
            return False
        else:
            return name in self._name_to_group

    def get_target_id(self, group_id: str) -> str | None:
        group = self._id_to_group.get(group_id)
        if group:
            return group.external_id
        else:
            return None

    def __len__(self):
        return len(self._name_to_group)


class GroupManager(CrawlerBase):
    _SYSTEM_GROUPS: typing.ClassVar[list[str]] = ["users", "admins", "account users"]

    def __init__(
        self,
        sql_backend: SqlBackend,
        ws: WorkspaceClient,
        inventory_database: str,
        include_group_names: list[str] | None = None,
        renamed_group_prefix: str = "ucx-renamed-",
    ):
        super().__init__(sql_backend, "hive_metastore", inventory_database, "groups", MigratedGroup)
        self._ws = ws
        self._include_group_names = include_group_names
        self._renamed_group_prefix = renamed_group_prefix

    def snapshot(self) -> list[MigratedGroup]:
        return self._snapshot(self._fetcher, self._crawler)

    def has_groups(self) -> bool:
        return len(self.snapshot()) > 0

    def rename_groups(self):
        tasks = []
        account_groups_in_workspace = self._account_groups_in_workspace()
        workspace_groups_in_workspace = self._workspace_groups_in_workspace()
        groups_to_migrate = self.get_migration_state().groups

        for mg in groups_to_migrate:
            if mg.name_in_account in account_groups_in_workspace:
                logger.info(f"Skipping {mg.name_in_account}: already in workspace")
                continue
            if mg.temporary_name in workspace_groups_in_workspace:
                logger.info(f"Skipping {mg.name_in_workspace}: already renamed")
                continue
            logger.info(f"Renaming: {mg.name_in_workspace} -> {mg.temporary_name}")
            tasks.append(functools.partial(self._rename_group, mg.id_in_workspace, mg.temporary_name))
        _, errors = Threads.gather("rename groups in the workspace", tasks)
        if len(errors) > 0:
            msg = f"During rename of workspace groups got {len(errors)} errors. See debug logs"
            raise RuntimeWarning(msg)

    def _rename_group(self, group_id: str, new_group_name: str):
        ops = [iam.Patch(iam.PatchOp.REPLACE, "displayName", new_group_name)]
        self._ws.groups.patch(group_id, operations=ops)
        return True

    def reflect_account_groups_on_workspace(self):
        tasks = []
        account_groups_in_account = self._account_groups_in_account()
        account_groups_in_workspace = self._account_groups_in_workspace()
        groups_to_migrate = self.get_migration_state().groups

        for mg in groups_to_migrate:
            if mg.name_in_account in account_groups_in_workspace:
                logger.info(f"Skipping {mg.name_in_account}: already in workspace")
                continue
            if mg.name_in_account not in account_groups_in_account:
                logger.warning(f"Skipping {mg.name_in_account}: not in account")
                continue
            group_id = account_groups_in_account[mg.name_in_account]
            tasks.append(functools.partial(self._reflect_account_group_to_workspace, group_id))
        _, errors = Threads.gather("reflect account groups on this workspace", tasks)
        if len(errors) > 0:
            msg = f"During account-to-workspace reflection got {len(errors)} errors. See debug logs"
            raise RuntimeWarning(msg)

    def get_migration_state(self) -> MigrationState:
        return MigrationState(self.snapshot())

    def delete_original_workspace_groups(self):
        tasks = []
        workspace_groups_in_workspace = self._workspace_groups_in_workspace()
        account_groups_in_workspace = self._account_groups_in_workspace()
        for mg in self.snapshot():
            if mg.temporary_name not in workspace_groups_in_workspace:
                logger.info(f"Skipping {mg.name_in_workspace}: no longer in workspace")
                continue
            if mg.name_in_account not in account_groups_in_workspace:
                logger.info(f"Skipping {mg.name_in_account}: not reflected in workspace")
                continue
            tasks.append(functools.partial(self._delete_workspace_group, mg.id_in_workspace, mg.temporary_name))
        _, errors = Threads.gather("removing original workspace groups", tasks)
        if len(errors) > 0:
            msg = f"During account-to-workspace reflection got {len(errors)} errors. See debug logs"
            raise RuntimeWarning(msg)

    def _fetcher(self) -> typing.Iterator[MigratedGroup]:
        for row in self._backend.fetch(f"SELECT * FROM {self._full_name}"):
            yield MigratedGroup(*row)

    def _crawler(self) -> typing.Iterator[MigratedGroup]:
        attributes = "id,displayName,meta,members,roles,entitlements"
        workspace_groups = self._list_workspace_groups("WorkspaceGroup", attributes)
        account_groups_in_account = self._account_groups_in_account()
        names_in_scope = self._get_valid_group_names_to_migrate(workspace_groups, account_groups_in_account)
        for g in workspace_groups:
            if g.display_name not in names_in_scope:
                logger.info(f"Skipping {g.display_name}: out of scope")
                continue
            temporary_name = f"{self._renamed_group_prefix}{g.display_name}"
            yield MigratedGroup(
                id_in_workspace=g.id,
                name_in_workspace=g.display_name,
                name_in_account=g.display_name,
                temporary_name=temporary_name,
                external_id=account_groups_in_account[g.display_name],
                members=json.dumps([gg.as_dict() for gg in g.members]) if g.members else None,
                roles=json.dumps([gg.as_dict() for gg in g.roles]) if g.roles else None,
                entitlements=json.dumps([gg.as_dict() for gg in g.entitlements]) if g.entitlements else None,
            )

    def _workspace_groups_in_workspace(self) -> dict[str, str]:
        by_name = {}
        for g in self._list_workspace_groups("WorkspaceGroup", "id,displayName,meta"):
            by_name[g.display_name] = g.id
        return by_name

    def _account_groups_in_workspace(self) -> dict[str, str]:
        by_name = {}
        for g in self._list_workspace_groups("Group", "id,displayName,meta"):
            by_name[g.display_name] = g.id
        return by_name

    def _account_groups_in_account(self) -> dict[str, str]:
        by_name = {}
        for g in self._list_account_groups("id,displayName"):
            by_name[g.display_name] = g.id
        return by_name

    def _is_group_out_of_scope(self, group: iam.Group, resource_type: str) -> bool:
        if group.display_name in self._SYSTEM_GROUPS:
            return True
        if group.meta.resource_type != resource_type:
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
            for g in self._ws.groups.list(attributes="id,displayName,meta"):
                if self._is_group_out_of_scope(g, resource_type):
                    continue
                group_with_all_attributes = self._get_group_with_retries(g.id)
                results.append(group_with_all_attributes)
        else:
            for g in self._ws.groups.list(attributes=scim_attributes):
                if self._is_group_out_of_scope(g, resource_type):
                    continue
                results.append(g)
        logger.info(f"Found {len(results)} {resource_type}")
        return results

    @retried(on=[InternalError], timeout=timedelta(minutes=2))
    @rate_limited(max_requests=255, burst_period_seconds=60)
    def _get_group_with_retries(self, group_id: str) -> iam.Group | None:
        return self._ws.groups.get(group_id)

    def _list_account_groups(self, scim_attributes: str) -> list[iam.Group]:
        # TODO: we should avoid using this method, as it's not documented
        # get account-level groups even if they're not (yet) assigned to a workspace
        logger.info(f"Listing account groups with {scim_attributes}...")
        account_groups = [
            iam.Group.from_dict(r)
            for r in self._ws.api_client.do(
                "get",
                "/api/2.0/account/scim/v2/Groups",
                query={"attributes": scim_attributes},
            ).get("Resources", [])
        ]
        account_groups = [g for g in account_groups if g.display_name not in self._SYSTEM_GROUPS]
        logger.info(f"Found {len(account_groups)} account groups")
        return sorted(account_groups, key=lambda _: _.display_name)

    @retried(on=[DatabricksError])
    @rate_limited(max_requests=35, burst_period_seconds=60)
    def _delete_workspace_group(self, group_id: str, display_name: str) -> None:
        try:
            logger.info(f"Deleting the workspace-level group {display_name} with id {group_id}")
            self._ws.groups.delete(id=group_id)
            logger.info(f"Workspace-level group {display_name} with id {group_id} was deleted")
            return True
        except DatabricksError as err:
            if "not found" in str(err):
                return True
            raise

    @retried(on=[DatabricksError])
    @rate_limited(max_requests=10)
    def _reflect_account_group_to_workspace(self, account_group_id: str):
        # TODO: add OpenAPI spec for it
        path = f"/api/2.0/preview/permissionassignments/principals/{account_group_id}"
        self._ws.api_client.do("PUT", path, data=json.dumps({"permissions": ["USER"]}))
        return True

    def _get_valid_group_names_to_migrate(
        self, workspace_groups: list[iam.Group], account_groups_in_account: dict[str, str]
    ) -> set[str]:
        if self._include_group_names:
            return self._validate_selected_groups(
                self._include_group_names, workspace_groups, account_groups_in_account
            )
        return self._detect_overlapping_group_names(workspace_groups, account_groups_in_account)

    @staticmethod
    def _detect_overlapping_group_names(
        workspace_groups: list[iam.Group], account_groups_in_account: dict[str, str]
    ) -> set[str]:
        logger.info(
            "No group listing provided, all available workspace-level groups that have an account-level "
            "group with the same name will be used"
        )
        ws_group_names = {_.display_name for _ in workspace_groups}
        ac_group_names = account_groups_in_account.keys()
        valid_group_names = ws_group_names.intersection(ac_group_names)
        logger.info(f"Found {len(valid_group_names)} workspace groups that have corresponding account groups")
        return valid_group_names

    @classmethod
    def _validate_selected_groups(
        cls, group_names: list[str], workspace_groups: list[iam.Group], account_groups_in_account: dict[str, str]
    ) -> set[str]:
        valid_group_names = set()
        ws_group_names = {_.display_name for _ in workspace_groups}
        logger.info("Using the provided group listing")
        for name in group_names:
            if name in cls._SYSTEM_GROUPS:
                logger.info(f"Cannot migrate system group {name}. {name} will be skipped.")
                continue
            if name not in ws_group_names:
                logger.info(f"Group {name} not found on the workspace level. {name} will be skipped.")
                continue
            if name not in account_groups_in_account:
                logger.info(
                    f"Group {name} not found on the account level. {name} will be skipped. You can add {name} "
                    f"to the account and rerun the job."
                )
                continue
            valid_group_names.add(name)
        return valid_group_names
