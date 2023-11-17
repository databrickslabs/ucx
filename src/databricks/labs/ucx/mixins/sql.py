import json
import logging
import random
import time
from collections.abc import Iterator
from datetime import timedelta

from databricks.sdk import errors
from databricks.sdk.errors import NotFound
from databricks.sdk.service.sql import (
    ColumnInfoTypeName,
    Disposition,
    ExecuteStatementResponse,
    Format,
    ResultData,
    ServiceErrorCode,
    StatementExecutionAPI,
    StatementState,
    StatementStatus,
)

MAX_SLEEP_PER_ATTEMPT = 10

MAX_PLATFORM_TIMEOUT = 50

MIN_PLATFORM_TIMEOUT = 5

_LOG = logging.getLogger("databricks.sdk")


class _RowCreator(tuple):
    def __new__(cls, fields):
        instance = super().__new__(cls, fields)
        return instance

    def __repr__(self):
        field_values = ", ".join(f"{field}={getattr(self, field)}" for field in self)
        return f"{self.__class__.__name__}({field_values})"


class Row(tuple):
    # Python SDK convention
    def as_dict(self) -> dict[str, any]:
        return dict(zip(self.__columns__, self, strict=True))

    # PySpark convention
    def __contains__(self, item):
        return item in self.__columns__

    def __getitem__(self, col):
        if isinstance(col, int | slice):
            return super().__getitem__(col)
        # if columns are named `2 + 2`, for example
        return self.__getattr__(col)

    def __getattr__(self, col):
        try:
            idx = self.__columns__.index(col)
            return self[idx]
        except IndexError:
            raise AttributeError(col)  # noqa: B904
        except ValueError:
            raise AttributeError(col)  # noqa: B904

    def __repr__(self):
        return f"Row({', '.join(f'{k}={v}' for (k, v) in zip(self.__columns__, self, strict=True))})"


class StatementExecutionExt(StatementExecutionAPI):
    def __init__(self, api_client):
        super().__init__(api_client)
        self.type_converters = {
            ColumnInfoTypeName.ARRAY: json.loads,
            # ColumnInfoTypeName.BINARY: not_supported(ColumnInfoTypeName.BINARY),
            ColumnInfoTypeName.BOOLEAN: bool,
            # ColumnInfoTypeName.BYTE: not_supported(ColumnInfoTypeName.BYTE),
            ColumnInfoTypeName.CHAR: str,
            # ColumnInfoTypeName.DATE: not_supported(ColumnInfoTypeName.DATE),
            ColumnInfoTypeName.DOUBLE: float,
            ColumnInfoTypeName.FLOAT: float,
            ColumnInfoTypeName.INT: int,
            # ColumnInfoTypeName.INTERVAL: not_supported(ColumnInfoTypeName.INTERVAL),
            ColumnInfoTypeName.LONG: int,
            ColumnInfoTypeName.MAP: json.loads,
            ColumnInfoTypeName.NULL: lambda _: None,
            ColumnInfoTypeName.SHORT: int,
            ColumnInfoTypeName.STRING: str,
            ColumnInfoTypeName.STRUCT: json.loads,
            # ColumnInfoTypeName.TIMESTAMP: not_supported(ColumnInfoTypeName.TIMESTAMP),
            # ColumnInfoTypeName.USER_DEFINED_TYPE: not_supported(ColumnInfoTypeName.USER_DEFINED_TYPE),
        }

    @staticmethod
    def _raise_if_needed(status: StatementStatus):
        if status.state not in [StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED]:
            return
        if "SCHEMA_NOT_FOUND" in status.error.message:
            raise NotFound(status.error.message)
        if "TABLE_NOT_FOUND" in status.error.message:
            raise NotFound(status.error.message)
        mapping = {
            ServiceErrorCode.ABORTED: errors.Aborted,
            ServiceErrorCode.ALREADY_EXISTS: errors.AlreadyExists,
            ServiceErrorCode.BAD_REQUEST: errors.BadRequest,
            ServiceErrorCode.CANCELLED: errors.Cancelled,
            ServiceErrorCode.DEADLINE_EXCEEDED: errors.DeadlineExceeded,
            ServiceErrorCode.INTERNAL_ERROR: errors.InternalError,
            ServiceErrorCode.IO_ERROR: errors.InternalError,
            ServiceErrorCode.NOT_FOUND: errors.NotFound,
            ServiceErrorCode.RESOURCE_EXHAUSTED: errors.ResourceExhausted,
            ServiceErrorCode.SERVICE_UNDER_MAINTENANCE: errors.TemporarilyUnavailable,
            ServiceErrorCode.TEMPORARILY_UNAVAILABLE: errors.TemporarilyUnavailable,
            ServiceErrorCode.UNAUTHENTICATED: errors.Unauthenticated,
            ServiceErrorCode.UNKNOWN: errors.Unknown,
            ServiceErrorCode.WORKSPACE_TEMPORARILY_UNAVAILABLE: errors.TemporarilyUnavailable,
        }
        error_class = mapping.get(status.error.error_code, errors.Unknown)
        raise error_class(status.error.message)

    def execute(
        self,
        warehouse_id: str,
        statement: str,
        *,
        byte_limit: int | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        timeout: timedelta = timedelta(minutes=20),
    ) -> ExecuteStatementResponse:
        # The wait_timeout field must be 0 seconds (disables wait),
        # or between 5 seconds and 50 seconds.
        wait_timeout = None
        if MIN_PLATFORM_TIMEOUT <= timeout.total_seconds() <= MAX_PLATFORM_TIMEOUT:
            # set server-side timeout
            wait_timeout = f"{timeout.total_seconds()}s"

        _LOG.debug(f"Executing SQL statement: {statement}")

        # technically, we can do Disposition.EXTERNAL_LINKS, but let's push it further away.
        # format is limited to Format.JSON_ARRAY, but other iterations may include ARROW_STREAM.
        immediate_response = self.execute_statement(
            warehouse_id=warehouse_id,
            statement=statement,
            catalog=catalog,
            schema=schema,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
            byte_limit=byte_limit,
            wait_timeout=wait_timeout,
        )

        if immediate_response.status.state == StatementState.SUCCEEDED:
            return immediate_response

        self._raise_if_needed(immediate_response.status)

        attempt = 1
        status_message = "polling..."
        deadline = time.time() + timeout.total_seconds()
        while time.time() < deadline:
            res = self.get_statement(immediate_response.statement_id)
            if res.status.state == StatementState.SUCCEEDED:
                return ExecuteStatementResponse(
                    manifest=res.manifest, result=res.result, statement_id=res.statement_id, status=res.status
                )
            status_message = f"current status: {res.status.state.value}"
            self._raise_if_needed(res.status)
            sleep = attempt
            if sleep > MAX_SLEEP_PER_ATTEMPT:
                # sleep 10s max per attempt
                sleep = MAX_SLEEP_PER_ATTEMPT
            _LOG.debug(f"SQL statement {res.statement_id}: {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        self.cancel_execution(immediate_response.statement_id)
        msg = f"timed out after {timeout}: {status_message}"
        raise TimeoutError(msg)

    def execute_fetch_all(
        self,
        warehouse_id: str,
        statement: str,
        *,
        byte_limit: int | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        timeout: timedelta = timedelta(minutes=20),
    ) -> Iterator[Row]:
        execute_response = self.execute(
            warehouse_id, statement, byte_limit=byte_limit, catalog=catalog, schema=schema, timeout=timeout
        )
        col_names = []
        col_conv = []
        for col in execute_response.manifest.schema.columns:
            col_names.append(col.name)
            conv = self.type_converters.get(col.type_name, None)
            if conv is None:
                msg = f"{col.name} has no {col.type_name.value} converter"
                raise ValueError(msg)
            col_conv.append(conv)
        row_factory = type("Row", (Row,), {"__columns__": col_names})
        result_data = execute_response.result
        if result_data is None:
            return []
        while True:
            for data in result_data.data_array:
                # enumerate() + iterator + tuple constructor makes it more performant
                # on larger humber of records for Python, even though it's less
                # readable code.
                row = []
                for i, value in enumerate(data):
                    if value is None:
                        row.append(None)
                    else:
                        row.append(col_conv[i](value))
                yield row_factory(row)
            if result_data.next_chunk_index is None:
                return
            # TODO: replace once ES-828324 is fixed
            json_response = self._api.do("GET", result_data.next_chunk_internal_link)
            result_data = ResultData.from_dict(json_response)
