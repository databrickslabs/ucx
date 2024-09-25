from __future__ import annotations
import dataclasses
import datetime as dt
import json
import logging
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar, Protocol, TypeVar

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient


logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class HistoricalRecord:
    workspace_id: int
    """The identifier of the workspace where this record was generated."""

    run_id: int
    """The identifier of the workflow run that generated this record."""

    snapshot_id: int
    """An identifier that is unique to the records produced for a given snapshot."""

    run_start_time: dt.datetime
    """When this record was generated."""

    object_type: str
    """The inventory table for which this record was generated."""

    object_type_version: int
    """Versioning of inventory table, for forward compatibility."""

    object_id: str
    """The type-specific identifier for this inventory record."""

    object_data: str
    """Type-specific JSON-encoded data of the inventory record."""

    failures: list[str]
    """The list of problems associated with the object that this inventory record covers."""

    owner: str
    """The identity of the account that created this inventory record."""


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict]


Record = TypeVar("Record", bound=DataclassInstance)


class HistoryLog:
    __slots__ = ("_ws", "_backend", "_run_id", "_catalog", "_schema", "_table")

    def __init__(
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
        run_id: int,
        catalog: str,
        schema: str,
        table: str,
    ) -> None:
        self._ws = ws
        self._backend = backend
        self._run_id = run_id
        self._catalog = catalog
        self._schema = schema
        self._table = table

    @property
    def full_name(self) -> str:
        return f"{self._catalog}.{self._schema}.{self._table}"

    def _append_history_snapshot(self, object_type: str, snapshot: list[HistoricalRecord]) -> None:
        logger.debug(f"[{self.full_name}] appending {len(snapshot)} new records for {object_type}")
        # Concurrent writes do not need to be handled here; appends cannot conflict.
        # TODO: Although documented as conflict-free, verify that this is truly is the case.
        self._backend.save_table(self.full_name, snapshot, HistoricalRecord, mode="append")

    class Appender:
        __slots__ = ("_ws", "_object_type", "_object_type_version", "_key_from", "_run_id", "_persist")

        def __init__(
            self,
            ws: WorkspaceClient,
            run_id: int,
            klass: type[Record],
            key_from: Callable[[Record], str],
            persist: Callable[[str, list[HistoricalRecord]], None],
        ) -> None:
            self._ws = ws
            self._run_id = run_id
            self._object_type = klass.__name__
            # Versioning support: if the dataclass has a _ucx_version class attribute that is the current version.
            self._object_type_version = getattr(klass, "_ucx_version") if hasattr(klass, "_ucx_version") else 0
            self._key_from = key_from
            self._persist = persist

        @cached_property
        def _workspace_id(self) -> int:
            return self._ws.get_workspace_id()

        @cached_property
        def _owner(self) -> str:
            current_user = self._ws.current_user.me()
            owner = current_user.user_name or current_user.id
            assert owner
            return owner

        def append_snapshot(self, records: Sequence[Record], *, run_start_time: dt.datetime) -> None:
            # Equivalent entropy to a type-4 UUID.
            snapshot_id = int.from_bytes(os.urandom(16), byteorder="big")
            historical_records = [
                self._inventory_record_to_historical(record, snapshot_id=snapshot_id, run_start_time=run_start_time)
                for record in records
            ]
            self._persist(self._object_type, historical_records)

        def _inventory_record_to_historical(
            self,
            record: Record,
            *,
            snapshot_id: int,
            run_start_time: dt.datetime,
        ) -> HistoricalRecord:
            object_id = self._key_from(record)
            object_as_dict = dataclasses.asdict(record)
            object_as_json = json.dumps(object_as_dict)
            # TODO: Get failures.
            failures: list[str] = []
            return HistoricalRecord(
                workspace_id=self._workspace_id,
                run_id=self._run_id,
                snapshot_id=snapshot_id,
                run_start_time=run_start_time,
                object_type=self._object_type,
                object_type_version=self._object_type_version,
                object_id=object_id,
                object_data=object_as_json,
                failures=failures,
                owner=self._owner,
            )

    def appender(self, klass: type[Record]) -> Appender:
        # TODO: Make a part of the protocol so the type-checker can enforce this.
        key_from = getattr(klass, "key_fields")
        return self.Appender(self._ws, self._run_id, klass, key_from, self._append_history_snapshot)
