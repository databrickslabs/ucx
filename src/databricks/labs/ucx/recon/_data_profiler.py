from collections.abc import Iterator

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row

from ._base import DataProfiler, DataProfilingResult, TableIdentifier, TableMetadataRetriever


class StandardDataProfiler(DataProfiler):
    def __init__(self, sql_backend: SqlBackend, metadata_retriever: TableMetadataRetriever):
        self._sql_backend = sql_backend
        self._metadata_retriever = metadata_retriever

    def profile_data(self, entity: TableIdentifier) -> DataProfilingResult:
        row_count = self._get_table_row_count(entity)
        return DataProfilingResult(
            row_count,
            self._metadata_retriever.get_metadata(entity),
        )

    def _get_table_row_count(self, entity: TableIdentifier) -> int:
        query_result: Iterator[Row] = self._sql_backend.fetch(
            f"SELECT COUNT(*) FROM {entity.table}",
            catalog=entity.catalog,
            schema=entity.schema,
        )
        count_row = next(query_result)
        return int(count_row[0])
