from abc import abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from logging import Logger

from databricks.sdk.service import iam, sql, workspace

from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.workspace_access.groups import MigrationState

logger = Logger(__name__)


# TODO: fix order to standard https://github.com/databrickslabs/ucx/issues/411
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
    def object_types(self) -> set[str]:
        """This method returns a set of strings, that represent object types that are applicable by this instance."""

    @abstractmethod
    def verify(
        self,
        object_type: str,
        object_id: str,
        acl: list[iam.AccessControlRequest | sql.AccessControl | iam.ComplexValue] | Grant | workspace.AclItem,
    ) -> bool:
        """This method verifies that all the crawled permissions are applied correctly to the destination group."""
