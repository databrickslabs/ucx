import enum
from abc import abstractmethod
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import partial
from logging import Logger
from typing import Literal

from databricks.labs.ucx.workspace_access.groups import GroupMigrationState

logger = Logger(__name__)


class StrEnum(str, enum.Enum):  # re-exported for compatability with older python versions
    def __new__(cls, value, *args, **kwargs):
        if not isinstance(value, str | enum.auto):
            msg = f"Values of StrEnums must be strings: {value!r} is a {type(value)}"
            raise TypeError(msg)
        return super().__new__(cls, value, *args, **kwargs)

    def __str__(self):
        return str(self.value)

    def _generate_next_value_(name, *_):  # noqa: N805
        return name


@dataclass
class Permissions:
    object_id: str
    object_type: str
    raw_object_permissions: str


Destination = Literal["backup", "account"]


class RequestObjectType(StrEnum):
    AUTHORIZATION = "authorization"  # tokens and passwords are here too!
    CLUSTERS = "clusters"
    CLUSTER_POLICIES = "cluster-policies"
    DIRECTORIES = "directories"
    EXPERIMENTS = "experiments"
    FILES = "files"
    INSTANCE_POOLS = "instance-pools"
    JOBS = "jobs"
    NOTEBOOKS = "notebooks"
    PIPELINES = "pipelines"
    REGISTERED_MODELS = "registered-models"
    REPOS = "repos"
    SQL_WAREHOUSES = "sql/warehouses"  # / is not a typo, it's the real object type

    def __repr__(self):
        return self.value


class Crawler:
    @abstractmethod
    def get_crawler_tasks(self) -> Iterator[Callable[..., Permissions | None]]:
        """
        This method should return a list of crawler tasks (e.g. partials or just any callables)
        :return:
        """


class Applier:
    @abstractmethod
    def is_item_relevant(self, item: Permissions, migration_state: GroupMigrationState) -> bool:
        """
        This method verifies that the given item is relevant for the given migration state.
        """

    @abstractmethod
    def _get_apply_task(
        self, item: Permissions, migration_state: GroupMigrationState, destination: Destination
    ) -> partial:
        """
        This method should return an instance of ApplierTask.
        """

    def get_apply_task(
        self, item: Permissions, migration_state: GroupMigrationState, destination: Destination
    ) -> partial:
        # we explicitly put the relevance check here to avoid "forgotten implementation" in child classes
        if self.is_item_relevant(item, migration_state):
            return self._get_apply_task(item, migration_state, destination)
        else:

            def noop():
                pass

            return partial(noop)
