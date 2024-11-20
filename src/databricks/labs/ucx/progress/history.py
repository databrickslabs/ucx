from __future__ import annotations
import dataclasses
import datetime as dt
import json
import logging
from enum import Enum, EnumMeta
from collections.abc import Iterable, Sequence
from typing import Any, ClassVar, Generic, Protocol, TypeVar, get_type_hints

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.progress.install import Historical


logger = logging.getLogger(__name__)


class DataclassWithIdAttributes(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Any]]

    __id_attributes__: ClassVar[tuple[str, ...]]
    """The names of attributes (can be dataclass fields or ordinary properties) that make up the object identifier.

    All attributes must be (non-optional) strings.
    """


Record = TypeVar("Record", bound=DataclassWithIdAttributes)
T = TypeVar("T")


class HistoricalEncoder(Generic[Record]):
    """An encoder for dataclasses that will be stored in our history log.

    Our history records are designed with several goals in mind:
     - Records can be of different types, meaning we have to store heterogenous types using a homogenous schema.
     - We partially shred the records to allow for easier SQL-based querying.
     - Flexibility from the start, because schema changes in the future will be very difficult.
     - Records will be shared (and queried) across workspaces with different versions of UCX installed.

    With this in mind:
     - We have an `object-type` (discriminator) field for holding the type of the record we're storing. This allows for
       type-specific logic when querying.
     - We have a (type-specific) field for storing the business key (identifier) of each record.
     - We have special handling for the `failures` attribute that is often present on the types we are storing.
     - The `object-data` is an unconstrained key-value map, allowing arbitrary attributes to be stored and (if top-level)
       queried directly. Complex values are JSON-encoded.
     - The :py:class:`Ownership` mechanism is used to associate each record with a user.
     - To help with forward and backward compatibility the UCX version used to encode a record is included in each
       record.
     - The associated workspace and job run is also included in each record.
     - These records are currently stored in a (UC-based) table that is shared across workspaces attached to the same
       metastore. They are used to help report on migration progress.
    """

    _job_run_id: int
    """The identifier of the current job run, with which these records are associated."""

    _workspace_id: int
    """The identifier of the current workspace, for identifying where these records came from."""

    _ownership: Ownership[Record]
    """Used to determine the owner for each record."""

    _object_type: str
    """The name of the record class being encoded by this instance."""

    _field_names_with_types: dict[str, type]
    """A map of the fields on instances of the record class (and their types); these will appear in the object data."""

    _has_failures: type[str | list[str]] | None
    """The type of the failures attribute for this record, if present."""

    _id_attribute_names: Sequence[str]
    """The names of the record attributes that are used to produce the identifier for each record.

    Attributes can be either a dataclass field or a property, for which the type must be a (non-optional) string.
    """

    def __init__(self, job_run_id: int, workspace_id: int, ownership: Ownership[Record], klass: type[Record]) -> None:
        self._job_run_id = job_run_id
        self._workspace_id = workspace_id
        self._ownership = ownership
        self._object_type = self._get_object_type(klass)
        self._field_names_with_types, self._has_failures = self._get_field_names_with_types(klass)
        self._id_attribute_names = self._get_id_attribute_names(klass)

    @classmethod
    def _get_field_names_with_types(cls, klass: type[Record]) -> tuple[dict[str, type], type[str | list[str]] | None]:
        """Return the dataclass-defined fields that the record type declares, and their associated types.

        If the record has a "failures" attribute this is treated specially: it is removed but we signal that it was
        present.

        Arguments:
            klass: The record type.
        Returns:
            A tuple containing:
                - A dictionary of fields to include in the object data, and their type.
                - The type of the failures field, if present.
        """
        # Ignore the field types returned by dataclasses.fields(): it doesn't resolve string-based annotations (which
        # are produced automatically in a __future__.__annotations__ context). Unfortunately the dataclass mechanism
        # captures the type hints prior to resolution (which happens later in the class initialization process).
        # As such, we rely on dataclasses.fields() for the set of field names, but not the types which we fetch directly.
        klass_type_hints = get_type_hints(klass)
        field_names = [field.name for field in dataclasses.fields(klass)]
        field_names_with_types = {field_name: klass_type_hints[field_name] for field_name in field_names}
        if "failures" not in field_names_with_types:
            failures_type = None
        else:
            failures_type = field_names_with_types.pop("failures")
            if failures_type not in (str, list[str]):
                msg = f"Historical record {klass} has invalid 'failures' attribute of type: {failures_type}"
                raise TypeError(msg)
        return field_names_with_types, failures_type

    def _get_id_attribute_names(self, klazz: type[Record]) -> Sequence[str]:
        id_attribute_names = tuple(klazz.__id_attributes__)
        all_fields = self._field_names_with_types
        for name in id_attribute_names:
            id_attribute_type = all_fields.get(name, None) or self._detect_property_type(klazz, name)
            if id_attribute_type is None:
                raise AttributeError(name=name, obj=klazz)
            if id_attribute_type != str:
                msg = f"Historical record {klazz} has a non-string id attribute: {name} (type={id_attribute_type})"
                raise TypeError(msg)
        return id_attribute_names

    def _detect_property_type(self, klazz: type[Record], name: str) -> str | None:
        maybe_property = getattr(klazz, name, None)
        if maybe_property is None:
            return None
        if not isinstance(maybe_property, property):
            msg = f"Historical record {klazz} declares an id attribute that is not a field or property: {name} (type={maybe_property})"
            raise TypeError(msg)
        property_getter = maybe_property.fget
        if not property_getter:
            msg = f"Historical record {klazz} has a non-readable property as an id attribute: {name}"
            raise TypeError(msg)
        type_hints = get_type_hints(property_getter) or {}
        try:
            return type_hints["return"]
        except KeyError as e:
            msg = f"Historical record {klazz} has a property with no type as an id attribute: {name}"
            raise TypeError(msg) from e

    @classmethod
    def _get_object_type(cls, klass: type[Record]) -> str:
        return klass.__name__

    @classmethod
    def _as_dict(cls, record: Record) -> dict[str, Any]:
        return dataclasses.asdict(record)

    def _object_id(self, record: Record) -> list[str]:
        return [getattr(record, field) for field in self._id_attribute_names]

    @classmethod
    def _encode_non_serializable(cls, name: str, value: Any) -> Any:
        if isinstance(type(value), EnumMeta):
            return cls._encode_enum(value)
        if isinstance(value, dt.datetime):
            return cls._encode_datetime(name, value)
        if isinstance(value, dt.date):
            return cls._encode_date(value)
        if isinstance(value, set):
            return cls._encode_set(value)

        msg = f"Cannot encode {type(value)} value in or within field {name}: {value!r}"
        raise TypeError(msg)

    @staticmethod
    def _encode_enum(value: Enum) -> str:
        """Enums are encoded as a string containing their name."""
        return value.name

    @staticmethod
    def _encode_datetime(name, value: dt.datetime) -> str:
        """Timestamps are encoded in ISO format, with a 'Z' timezone specifier. Naive timestamps aren't allowed."""
        # Only allow tz-aware timestamps.
        if value.tzinfo is None:
            # Name refers to the outermost field, not necessarily a field on a (nested) dataclass.
            msg = f"Timestamp without timezone not supported in or within field {name}: {value}"
            raise ValueError(msg)
        # Always store with 'Z'.
        ts_utc = value.astimezone(dt.timezone.utc)
        return ts_utc.isoformat().replace("+00:00", "Z")

    @staticmethod
    def _encode_date(value: dt.date) -> str:
        """Dates are encoded in ISO format."""
        return value.isoformat()

    @staticmethod
    def _encode_set(value: set[T]) -> list[T]:
        """Sets are encoded as an array of the elements."""
        return list(value)

    def _encode_field_value(self, name: str, value: Any) -> str | None:
        if value is None:
            return None
        value_type = self._field_names_with_types[name]
        if value_type in (str, (str | None)):
            if isinstance(value, str):
                return value
            msg = f"Invalid value for field {name}, not a string: {value!r}"
            raise ValueError(msg)
        encoded_value = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),  # More compact than default, which includes a space after the colon (:).
            default=lambda o: self._encode_non_serializable(name, o),
        )
        # Handle encoding substituted values that encode as just a string (eg. timestamps); we just return the string.
        return json.loads(encoded_value) if encoded_value.startswith('"') else encoded_value

    def _object_data_and_failures(self, record: Record) -> tuple[dict[str, str], list[str]]:
        record_values = self._as_dict(record)
        encoded_fields = {
            field: self._encode_field_value(field, record_values[field]) for field in self._field_names_with_types
        }
        # We must return a value: strings are mandatory (not optional) as the type. As such, optional fields need to be
        # omitted from the data map if the value is None.
        data = {k: v for k, v in encoded_fields.items() if v is not None}
        if self._has_failures == list[str]:
            failures = record_values["failures"]
        elif self._has_failures == str:
            raw_failures = record_values["failures"]
            try:
                failures = json.loads(raw_failures) if raw_failures else []
            except json.decoder.JSONDecodeError:
                failures = [raw_failures]
        else:
            failures = []

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


class ProgressEncoder(Generic[Record]):
    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: Ownership[Record],
        klass: type[Record],
        run_id: int,
        workspace_id: int,
        catalog: str,
        schema: str = "multiworkspace",
        table: str = "historical",
    ) -> None:
        self._sql_backend = sql_backend
        self._klass = klass
        self._catalog = catalog
        self._schema = schema
        self._table = table
        self._encoder = HistoricalEncoder(
            job_run_id=run_id,
            workspace_id=workspace_id,
            ownership=ownership,
            klass=klass,
        )

    @property
    def full_name(self) -> str:
        return f"{self._catalog}.{self._schema}.{self._table}"

    def append_inventory_snapshot(self, snapshot: Iterable[Record]) -> None:
        history_records = [self._encode_record_as_historical(record) for record in snapshot]
        logger.debug(f"Appending {len(history_records)} {self._klass} record(s) to history.")
        # The mode is 'append'. This is documented as conflict-free.
        self._sql_backend.save_table(escape_sql_identifier(self.full_name), history_records, Historical, mode="append")

    def _encode_record_as_historical(self, record: Record) -> Historical:
        """Encode a snapshot record as a historical log entry."""
        return self._encoder.to_historical(record)
