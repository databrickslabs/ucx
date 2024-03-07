import functools
import json
import logging
import random
import time
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

from databricks.sdk import WorkspaceClient, errors
from databricks.sdk.errors import DataLoss, NotFound
from databricks.sdk.service.sql import (
    ColumnInfoTypeName,
    Disposition,
    ExecuteStatementResponse,
    Format,
    ResultData,
    ServiceError,
    ServiceErrorCode,
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
    def as_dict(self) -> dict[str, Any]:
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
            raise AttributeError(col) from None
        except ValueError:
            raise AttributeError(col) from None

    def __repr__(self):
        return f"Row({', '.join(f'{k}={v}' for (k, v) in zip(self.__columns__, self, strict=True))})"


class StatementExecutionExt:
    def __init__(self, ws: WorkspaceClient):
        self._api = ws.api_client
        self.execute_statement = functools.partial(ws.statement_execution.execute_statement)
        self.cancel_execution = functools.partial(ws.statement_execution.cancel_execution)
        self.get_statement = functools.partial(ws.statement_execution.get_statement)
        self.type_converters = {
            ColumnInfoTypeName.ARRAY: json.loads,
            # ColumnInfoTypeName.BINARY: not_supported(ColumnInfoTypeName.BINARY),
            ColumnInfoTypeName.BOOLEAN: lambda value: value.lower() == "true",
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
        status_error = status.error
        if status_error is None:
            status_error = ServiceError(message="unknown", error_code=ServiceErrorCode.UNKNOWN)
        error_message = status_error.message
        if error_message is None:
            error_message = ""
        if "SCHEMA_NOT_FOUND" in error_message:
            raise NotFound(error_message)
        if "TABLE_OR_VIEW_NOT_FOUND" in error_message:
            raise NotFound(error_message)
        if "DELTA_TABLE_NOT_FOUND" in error_message:
            raise NotFound(error_message)
        if "DELTA_MISSING_TRANSACTION_LOG" in error_message:
            raise DataLoss(error_message)
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
        error_code = status_error.error_code
        if error_code is None:
            error_code = ServiceErrorCode.UNKNOWN
        error_class = mapping.get(error_code, errors.Unknown)
        raise error_class(error_message)

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

        status = immediate_response.status
        if status is None:
            status = StatementStatus(state=StatementState.FAILED)
        if status.state == StatementState.SUCCEEDED:
            return immediate_response

        self._raise_if_needed(status)

        attempt = 1
        status_message = "polling..."
        deadline = time.time() + timeout.total_seconds()
        statement_id = immediate_response.statement_id
        if not statement_id:
            msg = f"No statement id: {immediate_response}"
            raise ValueError(msg)
        while time.time() < deadline:
            res = self.get_statement(statement_id)
            result_status = res.status
            if not result_status:
                msg = f"Result status is none: {res}"
                raise ValueError(msg)
            state = result_status.state
            if not state:
                state = StatementState.FAILED
            if state == StatementState.SUCCEEDED:
                return ExecuteStatementResponse(
                    manifest=res.manifest, result=res.result, statement_id=statement_id, status=result_status
                )
            status_message = f"current status: {state.value}"
            self._raise_if_needed(result_status)
            sleep = min(attempt, MAX_SLEEP_PER_ATTEMPT)
            _LOG.debug(f"SQL statement {statement_id}: {status_message} (sleeping ~{sleep}s)")
            time.sleep(sleep + random.random())
            attempt += 1
        self.cancel_execution(statement_id)
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
        col_conv, row_factory = self._row_converters(execute_response)
        result_data = execute_response.result
        if result_data is None:
            return
        while True:  # pylint: disable=too-many-nested-blocks
            data_array = result_data.data_array
            if not data_array:
                data_array = []
            for data in data_array:
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
            if not result_data.next_chunk_internal_link:
                continue
            # TODO: replace once ES-828324 is fixed
            json_response = self._api.do("GET", result_data.next_chunk_internal_link)
            result_data = ResultData.from_dict(json_response)  # type: ignore[arg-type]

    def _row_converters(self, execute_response):
        col_names = []
        col_conv = []
        manifest = execute_response.manifest
        if not manifest:
            msg = f"missing manifest: {execute_response}"
            raise ValueError(msg)
        manifest_schema = manifest.schema
        if not manifest_schema:
            msg = f"missing schema: {manifest}"
            raise ValueError(msg)
        columns = manifest_schema.columns
        if not columns:
            columns = []
        for col in columns:
            col_names.append(col.name)
            type_name = col.type_name
            if not type_name:
                type_name = ColumnInfoTypeName.NULL
            conv = self.type_converters.get(type_name, None)
            if conv is None:
                msg = f"{col.name} has no {type_name.value} converter"
                raise ValueError(msg)
            col_conv.append(conv)
        row_factory = type("Row", (Row,), {"__columns__": col_names})
        return col_conv, row_factory
