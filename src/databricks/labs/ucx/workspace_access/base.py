from abc import abstractmethod
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from logging import Logger
from typing import Literal, Optional

from databricks.sdk.service.iam import AccessControlRequest, AccessControlResponse

from databricks.labs.ucx.workspace_access.groups import GroupMigrationState

logger = Logger(__name__)


# TODO: fix order to standard https://github.com/databrickslabs/ucx/issues/411
@dataclass
class Permissions:
    object_id: str
    object_type: str
    raw: str


Destination = Literal["backup", "account"]


class AclSupport:
    @abstractmethod
    def get_crawler_tasks(self) -> Iterator[Callable[..., Permissions | None]]:
        """
        This method should return a list of crawler tasks (e.g. partials or just any callables)
        :return:
        """

    @abstractmethod
    def get_apply_task(
        self, item: Permissions, migration_state: GroupMigrationState, destination: Destination
    ) -> Callable[[], None] | None:
        """This method returns a Callable, that applies permissions to a destination group, based on
        the group migration state. The callable is required not to have any shared mutable state."""

    @abstractmethod
    def object_types(self) -> set[str]:
        """This method returns a set of strings, that represent object types that are applicable by this instance."""
            def noop():
                pass

            return partial(noop)
