import functools
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from functools import cached_property
from typing import ClassVar, Generic, Protocol, TypeVar, final

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.service.iam import User

logger = logging.getLogger(__name__)


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict]


Record = TypeVar("Record")


class Ownership(ABC, Generic[Record]):
    """Determine an owner for a given type of object."""

    _cached_workspace_admins: dict[int, str | Exception] = {}
    """Cached user names of workspace administrators, keyed by workspace id."""

    @classmethod
    def reset_cache(cls) -> None:
        """Reset the cache of discovered administrators that we maintain at class level."""
        # Intended for use by tests.
        cls._cached_workspace_admins = {}

    def __init__(self, ws: WorkspaceClient) -> None:
        self._ws = ws

    @staticmethod
    def _has_role(user: User, role: str) -> bool:
        """Determine whether a user has a given role or not."""
        return user.roles is not None and any(r.value == role for r in user.roles)

    @staticmethod
    def _member_of_group_named(user: User, group_name: str) -> bool:
        """Determine whether a user belongs to a group with the given name or not."""
        return user.groups is not None and any(g.display == group_name for g in user.groups)

    @staticmethod
    def _member_of_group(user: User, group_id: str) -> bool:
        """Determine whether a user belongs to a group with the given identifier or not."""
        return user.groups is not None and any(g.value == group_id for g in user.groups)

    def _filter_workspace_groups(self, identifiers: Iterable[str]) -> Iterable[str]:
        """Limit a set of identifiers to those that are workspace groups."""
        seen = set()
        for group_id in identifiers:
            if group_id in seen:
                continue
            seen.add(group_id)
            try:
                group = self._ws.groups.get(group_id)
            except NotFound:
                continue
            if group.meta and group.meta.resource_type == "WorkspaceGroup":
                yield group_id

    def _find_workspace_admins(self) -> Iterable[User]:
        """Enumerate the active workspace administrators in a given workspace.

        Returns:
            Iterable[User]: The active workspace administrators, if any.
        """
        logger.debug("Enumerating users to locate active workspace administrators...")
        all_users = self._ws.users.list(attributes="id,active,userName,groups")
        # The groups attribute is a flattened list of groups a user belongs to; hunt for the 'admins' workspace group.
        # Reference: https://learn.microsoft.com/en-us/azure/databricks/admin/users-groups/groups#account-vs-workspace-group
        admin_users = [
            user for user in all_users if user.active and user.user_name and self._member_of_group_named(user, "admins")
        ]
        logger.debug(f"Verifying membership of the 'admins' workspace group for users: {admin_users}")
        candidate_group_ids = (
            group.value
            for user in admin_users
            if user.groups
            for group in user.groups
            if group.display == "admins" and group.value
        )
        admin_groups = list(self._filter_workspace_groups(candidate_group_ids))
        match admin_groups:
            case []:
                return ()
            case [admin_group]:
                return (user for user in admin_users if self._member_of_group(user, admin_group))
            case _:
                msg = f"Multiple 'admins' workspace groups found; something is wrong: {admin_groups}"
                raise RuntimeError(msg)

    def _find_account_admins(self) -> Iterable[User]:
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

    def _find_an_admin(self) -> User | None:
        """Locate an active administrator for the current workspace.

        If an active workspace administrator can be located, this is returned. When there are multiple, they are sorted
        alphabetically by user-name and the first is returned. If there are no workspace administrators then an active
        account administrator is sought, again returning the first alphabetically by user-name if there is more than one.

        Returns:
            the first (alphabetically by user-name) active workspace or account administrator, or `None` if neither can
            be found.
        """
        first_user = functools.partial(min, default=None, key=lambda user: user.user_name)
        return first_user(self._find_workspace_admins()) or first_user(self._find_account_admins())

    @final
    def owner_of(self, record: Record) -> str:
        """Obtain the user-name of a user that is responsible for the given record.

        This is intended to be a point of contact, and is either:

         - The user that originally created the resource associated with the result; or
         - An active administrator for the current workspace.

        Args:
            record (Record): The record for which an associated user-name is sought.
        Returns:
            A string containing the user-name attribute of the user considered to own the resource.
        Raises:
            RuntimeError if there are no active administrators for the current workspace.
        """
        return self._get_owner(record) or self._workspace_admin

    @cached_property
    def _workspace_admin(self) -> str:
        # Avoid repeatedly hitting the shared cache.
        return self._find_an_administrator()

    @final
    def _find_an_administrator(self) -> str:
        # Finding an administrator is quite expensive, so we ensure that for a given workspace we only do it once.
        # Found administrators are cached on a class attribute. The method here:
        #  - is thread-safe, with the compromise that we might perform some redundant lookups during init.
        #  - no administrator is converted into an error.
        #  - an error during lookup is preserved and raised for subsequent requests, to avoid too many REST calls.
        workspace_id = self._ws.get_workspace_id()
        found_admin_or_error = self._cached_workspace_admins.get(workspace_id, None)
        if found_admin_or_error is None:
            logger.debug(f"Locating an active workspace or account administrator for workspace: {workspace_id}")
            try:
                user = self._find_an_admin()
            except DatabricksError as e:
                found_admin_or_error = e
            else:
                found_admin_or_error = user.user_name if user is not None else None
                # If not found, convert once into the error that we will raise each time.
                if found_admin_or_error is None:
                    msg = f"No active workspace or account administrator can be found for workspace: {workspace_id}"
                    found_admin_or_error = RuntimeError(msg)  # pylint: disable=redefined-variable-type
            self._cached_workspace_admins[workspace_id] = found_admin_or_error
        if isinstance(found_admin_or_error, Exception):
            raise found_admin_or_error
        return found_admin_or_error

    @abstractmethod
    def _get_owner(self, record: Record) -> str | None:
        """Obtain the record-specific user-name associated with the given result, if any."""
        return None
