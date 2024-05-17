from collections.abc import Iterator

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row

from .base import DataProfiler, DataProfilingResult, TableIdentifier, TableMetadataRetriever


class StandardDataProfiler(DataProfiler):
    def __init__(self, sql_backend: SqlBackend, metadata_retriever: TableMetadataRetriever):
        self._sql_backend = sql_backend
        self._metadata_retriever = metadata_retriever

    def profile_data(self, entity: TableIdentifier) -> DataProfilingResult:
        """
        This method profiles the data in the given table. It takes a TableIdentifier object as input, which represents
        the table to be profiled. The method performs two main operations:

        1. It retrieves the row count of the table.
        2. It retrieves the metadata of the table using a TableMetadataRetriever instance.

        Note: This method does not handle exceptions raised during the execution of the SQL query or the retrieval
        of the table metadata. These exceptions are expected to be handled by the caller
        in a manner appropriate for their context.
        """
        row_count = self._get_table_row_count(entity)
        return DataProfilingResult(
            row_count,
            self._metadata_retriever.get_metadata(entity),
        )

    def _get_table_row_count(self, entity: TableIdentifier) -> int:
        query_result: Iterator[Row] = self._sql_backend.fetch(f"SELECT COUNT(*) as row_count FROM {entity.fqn_escaped}")
        count_row = next(query_result)
        return int(count_row[0])
