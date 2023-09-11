from abc import ABC, abstractmethod
from collections.abc import Callable
from functools import partial
from logging import Logger

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.inventory.types import Destination, PermissionsInventoryItem
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.utils import noop

logger = Logger(__name__)


class Crawler:
    @abstractmethod
    def get_crawler_tasks(self) -> list[Callable[..., PermissionsInventoryItem | None]]:
        pass


class Applier:
    @abstractmethod
    def is_item_relevant(self, item: PermissionsInventoryItem, migration_state: GroupMigrationState) -> bool:
        pass

    @abstractmethod
    def _get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ) -> partial:
        """
        This method should return an instance of ApplierTask.
        """

    def get_apply_task(
        self, item: PermissionsInventoryItem, migration_state: GroupMigrationState, destination: Destination
    ) -> partial:
        # we explicitly put the relevance check here to avoid "forgotten implementation" in child classes
        if self.is_item_relevant(item, migration_state):
            return self._get_apply_task(item, migration_state, destination)
        else:
            return partial(noop)


class BaseSupport(ABC, Crawler, Applier):
    """
    Base class for all support classes.
    Child classes must implement all abstract methods.
    """

    def __init__(self, ws: WorkspaceClient):
        # workspace client is required in all implementations
        self._ws = ws
