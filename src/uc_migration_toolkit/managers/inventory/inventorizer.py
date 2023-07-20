import concurrent
import datetime as dt
from asyncio import ALL_COMPLETED
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Generic, TypeVar

from databricks.sdk.service.iam import ObjectPermissions
from ratelimit import limits, sleep_and_retry

from uc_migration_toolkit.managers.inventory.types import (
    LogicalObjectType,
    PermissionsInventoryItem,
    RequestObjectType,
)
from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger

InventoryObject = TypeVar("InventoryObject")


class StandardInventorizer(Generic[InventoryObject]):
    """
    Standard means that it can collect using the default listing/permissions function without any additional logic.
    """

    def __init__(
        self,
        logical_object_type: LogicalObjectType,
        request_object_type: RequestObjectType,
        listing_function: Callable[..., Iterator[InventoryObject]],
        id_attribute: str,
        permissions_function: Callable[..., ObjectPermissions] | None = None,
    ):
        self._config = config_provider.config.rate_limit
        self._logical_object_type = logical_object_type
        self._request_object_type = request_object_type
        self._listing_function = listing_function
        self._id_attribute = id_attribute
        self._permissions_function = permissions_function if permissions_function else provider.ws.permissions.get
        self._objects: list[InventoryObject] = []

    @property
    def logical_object_type(self) -> LogicalObjectType:
        return self._logical_object_type

    def preload(self):
        logger.info(f"Listing objects with type {self._request_object_type}...")
        self._objects = list(self._listing_function())
        logger.info(f"Object metadata prepared for {len(self._objects)} objects.")

    def _process_single_object(self, _object: InventoryObject) -> PermissionsInventoryItem:
        permissions = self._permissions_function(
            self._request_object_type, _object.__getattribute__(self._id_attribute)
        )
        inventory_item = PermissionsInventoryItem(
            object_id=_object.__getattribute__(self._id_attribute),
            logical_object_type=self._logical_object_type,
            request_object_type=self._request_object_type,
            object_permissions=permissions.as_dict(),
        )
        return inventory_item

    def inventorize(self):
        logger.info(f"Fetching permissions for {len(self._objects)} objects...")
        futures = []
        counter = 0
        start_time = dt.datetime.now()

        def progress_report(_):
            nonlocal counter
            counter += 1
            measuring_time = dt.datetime.now()
            delta_from_start = measuring_time - start_time
            rps = counter / delta_from_start.total_seconds()
            offset = len(str(len(self._objects)))
            if counter % 10 == 0 or counter == len(self._objects):
                logger.info(f"fetch status - fetched: {counter:>{offset}d}/{len(self._objects)}, rps: {rps:.3f}/sec")

        @sleep_and_retry
        @limits(calls=self._config.max_requests_per_period, period=self._config.period_in_seconds)
        def process_singe_object(_object: InventoryObject):
            self._process_single_object(_object)

        with ThreadPoolExecutor(self._config.num_threads) as executor:
            for _object in self._objects:
                future = executor.submit(self._process_single_object, _object)
                future.add_done_callback(progress_report)
                futures.append(future)

            results = concurrent.futures.wait(futures, return_when=ALL_COMPLETED)

        collected: list[PermissionsInventoryItem] = [future.result() for future in results.done]

        logger.info(f"Permissions fetched for {len(collected)} objects of type {self._request_object_type}")
        return collected
