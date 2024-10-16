from __future__ import annotations
import dataclasses
import datetime as dt
import json
import logging
from collections.abc import Callable, Iterable, Sequence
from typing import ClassVar, Protocol, TypeVar, Generic, Any

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.progress.install import Historical

logger = logging.getLogger(__name__)


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Any]]

    __id_fields__: ClassVar[Sequence[str]]


Record = TypeVar("Record", bound=DataclassInstance)


class HistoricalEncoder(Generic[Record]):
    def __init__(self, job_run_id: int, workspace_id: int, ownership: Ownership[Record], klass: type[Record]) -> None:
        self._job_run_id = job_run_id
        self._workspace_id = workspace_id
        self._ownership = ownership
        self._object_type = self._get_object_type(klass)
        self._field_types, self._has_failures = self._get_field_types(klass)
        self._id_field_names = self._get_id_fieldnames(klass)

    @classmethod
    def _get_field_types(cls, klass: type[Record]) -> tuple[dict[str, type], bool]:
        field_types = {field.name: field.type for field in dataclasses.fields(klass)}
        failures_type = field_types.pop("failures", None)
        has_failures = failures_type is not None
        if has_failures and failures_type != list[str]:
            msg = f"Historical record class has invalid failures type: {failures_type}"
            raise TypeError(msg)
        return field_types, has_failures

    def _get_id_fieldnames(self, klazz: type[Record]) -> Sequence[str]:
        id_field_names = tuple(klazz.__id_fields__)
        all_fields = self._field_types
        for field in id_field_names:
            id_field_type = all_fields.get(field, None)
            if id_field_type is None:
                raise AttributeError(name=field, obj=klazz)
            if id_field_type != str:
                msg = f"Historical record class id field is not a string: {field}"
                raise TypeError(msg)
        return id_field_names

    @classmethod
    def _get_object_type(cls, klass: type[Record]) -> str:
        return klass.__name__

    @classmethod
    def _as_dict(cls, record: Record) -> dict[str, Any]:
        return dataclasses.asdict(record)

    def _object_id(self, record: Record) -> list[str]:
        return [getattr(record, field) for field in self._id_field_names]

    @classmethod
    def _encode_non_serializable(cls, name: str, value: Any) -> Any:
        if isinstance(value, dt.datetime):
            # Only allow tz-aware timestamps.
            if value.tzinfo is None:
                msg = f"Timestamp without timezone not supported for field {name}: {value}"
                raise ValueError(msg)
            # Always store with 'Z'.
            ts_utc = value.astimezone(dt.timezone.utc)
            return ts_utc.isoformat().replace("+00:00", "Z")

        msg = f"Cannot encode value for {name}: {value!r}"
        raise TypeError(msg)

    def _encode_field_value(self, name: str, value: Any | None) -> str | None:
        if value is None:
            return None
        value_type = self._field_types[name]
        # TODO: Update to check this covers str | None
        if value_type == str:
            return value
        encoded_value = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            default=lambda o: self._encode_non_serializable(name, o),
        )
        # Handle encoding substituted values that encode as just a string (eg. timestamps); we just return the string.
        return json.loads(encoded_value) if encoded_value.startswith('"') else encoded_value

    def _object_data_and_failures(self, record: Record) -> tuple[dict[str, str], list[str]]:
        record_values = self._as_dict(record)
        encoded_fields = {field: self._encode_field_value(field, record_values[field]) for field in self._field_types}
        # We must return a value: strings are mandatory (not optional) as the type. As such, optional fields need to be
        # omitted from the data map if the value is None.
        data = {k: v for k, v in encoded_fields.items() if v is not None}
        failures = record_values["failures"] if self._has_failures else []

        return data, failures

    def to_historical(self, record: Record) -> Historical:
        data, failures = self._object_data_and_failures(record)
        return Historical(
            workspace_id=self._workspace_id,
            job_run_id=self._job_run_id,
            object_type=self._object_type,
            object_id=self._object_id(record),
            data=data,
            failures=failures,
            owner=self._ownership.owner_of(record),
        )


class HistoryLog(Generic[Record]):
    def __init__(  # pylint: disable=too-many-arguments
        self,
        ws: WorkspaceClient,
        backend: SqlBackend,
        ownership: Ownership[Record],
        klass: type[Record],
        run_id: int,
        workspace_id: int,
        catalog: str,
        schema: str = "ucx",
        table: str = "history",
    ) -> None:
        self._ws = ws
        self._backend = backend
        self._klass = klass
        self._catalog = catalog
        self._schema = schema
        self._table = table
        encoder = HistoricalEncoder(job_run_id=run_id, workspace_id=workspace_id, ownership=ownership, klass=klass)
        self._encoder = encoder

    @staticmethod
    def _record_identifier(record_type: type[Record]) -> Callable[[Record], list[str]]:
        key_field_names = getattr(record_type, "key_fields", ())

        def identify_record(record: Record) -> list[str]:
            return [getattr(record, key_field) for key_field in key_field_names]

        return identify_record

    @property
    def full_name(self) -> str:
        return f"{self._catalog}.{self._schema}.{self._table}"

    def append_inventory_snapshot(self, snapshot: Sequence[Record]) -> None:
        history_records = list(self._inventory_records_to_historical(snapshot))
        # This is the only writer, and the mode is 'append'. This is documented as conflict-free.
        self._backend.save_table(escape_sql_identifier(self.full_name), history_records, Historical, mode="append")

    def _inventory_records_to_historical(self, records: Sequence[Record]) -> Iterable[Historical]:
        for record in records:
            yield self._encoder.to_historical(record)
