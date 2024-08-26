from abc import abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from logging import Logger

from databricks.labs.ucx.workspace_access.groups import MigrationState

logger = Logger(__name__)


@dataclass
class Permissions:
    object_id: str
    object_type: str
    raw: str


class AclSupport:
    @abstractmethod
    def get_crawler_tasks(self) -> Iterable[Callable[..., Permissions | None]]:
        """
        This method should return a list of crawler tasks (e.g. partials or just any callables)
        :return:
        """

    @abstractmethod
    def get_apply_task(self, item: Permissions, migration_state: MigrationState) -> Callable[[], None] | None:
        """This method returns a Callable, that applies permissions to a destination group, based on
        the group migration state. The callable is required not to have any shared mutable state."""

    @abstractmethod
    def get_verify_task(self, item: Permissions) -> Callable[[], bool] | None:
        """This method returns a Callable that verifies that all the crawled permissions are applied correctly to the
        destination group."""

    @abstractmethod
    def object_types(self) -> set[str]:
        """This method returns a set of strings, that represent object types that are applicable by this instance."""


class StaticListing:
    """This class is only supposed to be used in testing scenarios.
    It returns a static list of permissions specific object types, that can be used to test the ACL support classes."""

    def __init__(self, include_object_permissions: list[str], object_types: set[str]):
        self._include_object_permissions = include_object_permissions
        self._object_types = object_types

    def __iter__(self):
        for pair in self._include_object_permissions:
            object_type, object_id = pair.split(":")
            if object_type not in self._object_types:
                continue
            yield Permissions(object_id, object_type, '')

    def __repr__(self):
        return f"StaticListing({self._include_object_permissions})"
