import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Sequence
from datetime import timedelta
from functools import cached_property
from typing import Generic, TypeVar, final

from databricks.labs.blueprint.paths import WorkspacePath
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError, InvalidParameterValue, NotFound, ResourceDoesNotExist
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import User, ObjectPermissions, PermissionLevel
from databricks.sdk.service.workspace import ObjectType

logger = logging.getLogger(__name__)


Record = TypeVar("Record")


class AdministratorFinder(ABC):
    def __init__(self, ws: WorkspaceClient) -> None:
        self._ws = ws

    @abstractmethod
    def find_admin_users(self) -> Iterable[User]:
        """Locate active admin users."""
        raise NotImplementedError()


class WorkspaceAdministratorFinder(AdministratorFinder):
    """Locate the users that are in the 'admin' workspace group for a given workspace."""

    @staticmethod
    def _member_of_group_named(user: User, group_name: str) -> bool:
        """Determine whether a user belongs to a group with the given name or not."""
        return user.groups is not None and any(g.display == group_name for g in user.groups)

    @staticmethod
    def _member_of_group(user: User, group_id: str) -> bool:
        """Determine whether a user belongs to a group with the given identifier or not."""
        return user.groups is not None and any(g.value == group_id for g in user.groups)

    def _is_active_admin(self, user: User) -> bool:
        """Determine if a user is an active administrator."""
        return bool(user.active) and self._member_of_group_named(user, "admins")

    def _is_workspace_group(self, group_id: str) -> bool:
        """Determine whether a group_id corresponds to a workspace group or not."""
        try:
            group = self._ws.groups.get(group_id)
        except NotFound:
            return False
        return bool(group.meta and group.meta.resource_type == "WorkspaceGroup")

    def find_admin_users(self) -> Iterable[User]:
        """Enumerate the active workspace administrators in a given workspace.

        Returns:
            Iterable[User]: The active workspace administrators, if any.
        """
        logger.debug("Enumerating users to locate active workspace administrators...")
        all_users = self._ws.users.list(attributes="id,active,userName,groups")
        # The groups attribute is a flattened list of groups a user belongs to; hunt for the 'admins' workspace group.
        # Reference: https://learn.microsoft.com/en-us/azure/databricks/admin/users-groups/groups#account-vs-workspace-group
        admin_users = [user for user in all_users if user.user_name and self._is_active_admin(user)]
        logger.debug(f"Verifying membership of the 'admins' workspace group for users: {admin_users}")
        maybe_admins_id = set()
        for user in admin_users:
            if not user.groups:
                continue
            for group in user.groups:
                if group.display == "admins" and group.value:
                    maybe_admins_id.add(group.value)
        # There can only be a single 'admins' workspace group.
        for group_id in maybe_admins_id:
            if self._is_workspace_group(group_id):
                return (user for user in admin_users if self._member_of_group(user, group_id))
        return ()


class AccountAdministratorFinder(AdministratorFinder):
    """Locate the users that are account administrators for this workspace."""

    @staticmethod
    def _has_role(user: User, role: str) -> bool:
        """Determine whether a user has a given role or not."""
        return user.roles is not None and any(r.value == role for r in user.roles)

    def find_admin_users(self) -> Iterable[User]:
        """Enumerate the active account administrators associated with a given workspace.

        Returns:
            Iterable[User]: The active account administrators, if any.
        """
        logger.debug("Enumerating account users to locate active administrators...")
        response = self._ws.api_client.do(
            "GET", "/api/2.0/account/scim/v2/Users", query={"attributes": "id,active,userName,roles"}
        )
        assert isinstance(response, dict)
        all_users = (User.from_dict(resource) for resource in response.get("Resources", []))
        # Reference: https://learn.microsoft.com/en-us/azure/databricks/admin/users-groups/groups#account-admin
        return (user for user in all_users if user.active and user.user_name and self._has_role(user, "account_admin"))


class AdministratorLocator:
    """Locate a workspace administrator, if possible.

    This will first try to find an active workspace administrator. If there are multiple, the first (alphabetically
    sorted by user-name) will be used. If no active workspace administrators can be found then an account administrator
    is sought, again returning the first alphabetically by user-name if more than one is found.
    """

    def __init__(
        self,
        ws: WorkspaceClient,
        *,
        finders: Sequence[Callable[[WorkspaceClient], AdministratorFinder]] = (
            WorkspaceAdministratorFinder,
            AccountAdministratorFinder,
        ),
    ) -> None:
        """
        Initialize the instance, which will try to locate administrators using the workspace for the supplied client.

        Args:
            ws (WorkspaceClient): the client for workspace in which to locate admin users.
            finders: a sequence of factories that will be instantiated on demand to locate admin users.
        """
        self._ws = ws
        self._finders = finders

    @cached_property
    def _workspace_id(self) -> int:
        # Makes a REST call, so we cache it.
        return self._ws.get_workspace_id()

    @cached_property
    def _found_admin(self) -> str | None:

        # Ordering helper: User.user_name is typed as optional but we can't sort by None.
        # (The finders already filter out users without a user-name.)
        def _by_username(user: User) -> str:
            assert user.user_name
            return user.user_name

        # Lazily instantiate and query the finders in an attempt to locate an admin user.
        for factory in self._finders:
            finder = factory(self._ws)
            # First alphabetically by name.
            admin_user = min(finder.find_admin_users(), default=None, key=_by_username)
            if admin_user:
                return admin_user.user_name
        return None

    def get_workspace_administrator(self) -> str:
        """The user-name of an admin user for the workspace.

        Raises:
              RuntimeError if an admin user cannot be found in the current workspace.
        """
        found_admin = self._found_admin
        if found_admin is None:
            msg = f"No active workspace or account administrator can be found for workspace: {self._workspace_id}"
            raise RuntimeError(msg)
        return found_admin


class Ownership(ABC, Generic[Record]):
    """Determine an owner for a given type of object."""

    def __init__(self, administrator_locator: AdministratorLocator) -> None:
        self._administrator_locator = administrator_locator

    @final
    def owner_of(self, record: Record) -> str:
        """Obtain the user-name of a user that is responsible for the given record.

        This is intended to be a point of contact, and is either:

         - A user directly associated with the resource, such as the original creator; or
         - An active administrator for the current workspace.

        Args:
            record (Record): The record for which an associated user-name is sought.
        Returns:
            A string containing the user-name attribute of a user considered to be responsible for the resource.
        Raises:
            RuntimeError if there are no active administrators for the current workspace.
        """
        return self._maybe_direct_owner(record) or self._administrator_locator.get_workspace_administrator()

    @abstractmethod
    def _maybe_direct_owner(self, record: Record) -> str | None:
        """Obtain the record-specific user-name associated with the given record, if any."""
        return None


class WorkspacePathOwnership(Ownership[WorkspacePath]):
    def __init__(self, administrator_locator: AdministratorLocator, ws: WorkspaceClient) -> None:
        super().__init__(administrator_locator)
        self._ws = ws

    def owner_of_path(self, path: str) -> str:
        return self.owner_of(WorkspacePath(self._ws, path))

    @retried(on=[InternalError], timeout=timedelta(minutes=1))
    def _maybe_direct_owner(self, record: WorkspacePath) -> str | None:
        maybe_type_and_id = self._maybe_type_and_id(record)
        if not maybe_type_and_id:
            return None
        object_type, object_id = maybe_type_and_id
        try:
            object_permissions = self._ws.permissions.get(object_type, object_id)
            return self._infer_from_first_can_manage(object_permissions)
        except NotFound:
            logger.warning(f"removed on backend: {object_type} {object_id}")
            return None

    @staticmethod
    def _maybe_type_and_id(path: WorkspacePath) -> tuple[str, str] | None:
        try:
            object_info = path._object_info  # pylint: disable=protected-access
        except (InvalidParameterValue, ResourceDoesNotExist):
            logger.warning(f"Cannot retrieve status for: {path}")
            return None
        if not (object_info.object_id and object_info.object_type):
            return None
        object_id = str(object_info.object_id)
        match object_info.object_type:
            case ObjectType.NOTEBOOK:
                return 'notebooks', object_id
            case ObjectType.FILE:
                return 'files', object_id
            case ObjectType.DIRECTORY:
                return 'directories', object_id
            case _:
                logger.warning(f"Unsupported object type: {object_info.object_type.value}")
        return None

    @staticmethod
    def _infer_from_first_can_manage(object_permissions: ObjectPermissions) -> str | None:
        if object_permissions.access_control_list is None:
            return None
        for acl in object_permissions.access_control_list:
            if acl.all_permissions is None:
                return None
            for permission in acl.all_permissions:
                if permission.permission_level != PermissionLevel.CAN_MANAGE:
                    continue
                if acl.user_name:
                    return acl.user_name
                if acl.group_name:
                    return acl.group_name
                return acl.service_principal_name
        return None


class LegacyQueryOwnership(Ownership[str]):
    def __init__(self, administrator_locator: AdministratorLocator, workspace_client: WorkspaceClient) -> None:
        super().__init__(administrator_locator)
        self._workspace_client = workspace_client

    def _maybe_direct_owner(self, record: str) -> str | None:
        try:
            legacy_query = self._workspace_client.queries.get(record)
            return legacy_query.owner_user_name
        except NotFound:
            return None
        except InternalError:  # redash is very naughty and throws 500s instead of proper 404s
            return None
