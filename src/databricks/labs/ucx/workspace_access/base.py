from abc import abstractmethod
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import partial
from logging import Logger
from typing import Literal

from databricks.labs.ucx.workspace_access.groups import GroupMigrationState

logger = Logger(__name__)


@dataclass
class Permissions:
    object_id: str
    object_type: str
    raw: str


Destination = Literal["backup", "account"]


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
