from __future__ import annotations
import dataclasses
import datetime as dt
import json
import logging
import os
from collections.abc import Callable, Iterable
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

    object_id: list[str]
    """The type-specific identifier for this inventory record."""

    object_data: dict[str, str]
    """Type-specific data of the inventory record. Keys are top-level attributes, values are their JSON-encoded values."""

    failures: list[str]
    """The list of problems associated with the object that this inventory record covers."""

    owner: str
    """The identity of the account that created this inventory record."""


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict]
    # TODO: Once all record types provide the property: key_fields: ClassVar[Sequence[str]]


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
            key_from: Callable[[Record], list[str]],
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

        def append_snapshot(self, records: Iterable[Record], *, run_start_time: dt.datetime) -> None:
            snapshot_id: int = int.from_bytes(os.urandom(7), byteorder="big")
            historical_records = list(
                self._inventory_records_to_historical(
                    records,
                    snapshot_id=snapshot_id,
                    run_start_time=run_start_time,
                )
            )
            self._persist(self._object_type, historical_records)

        def _inventory_records_to_historical(
            self,
            records: Iterable[Record],
            *,
            snapshot_id: int,
            run_start_time: dt.datetime,
        ) -> Iterable[HistoricalRecord]:
            for record in records:
                object_id = self._key_from(record)
                object_as_dict = dataclasses.asdict(record)
                flattened_object_data = {k: json.dumps(v) for k, v in object_as_dict.items()}
                # TODO: Get failures.
                failures: list[str] = []
                yield HistoricalRecord(
                    workspace_id=self._workspace_id,
                    run_id=self._run_id,
                    snapshot_id=snapshot_id,
                    run_start_time=run_start_time,
                    object_type=self._object_type,
                    object_type_version=self._object_type_version,
                    object_id=object_id,
                    object_data=flattened_object_data,
                    failures=failures,
                    owner=self._owner,
                )

    def appender(self, klass: type[Record]) -> Appender:
        key_fields = getattr(klass, "key_fields", ())

        def key_from(record: Record) -> list[str]:
            return [getattr(record, field) for field in key_fields]

        return self.Appender(self._ws, self._run_id, klass, key_from, self._append_history_snapshot)
