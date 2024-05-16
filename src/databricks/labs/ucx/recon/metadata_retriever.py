from collections.abc import Iterator

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row

from .base import TableIdentifier, TableMetadata, ColumnMetadata, TableMetadataRetriever
from .query_builder import build_metadata_query


class DatabricksTableMetadataRetriever(TableMetadataRetriever):
    def __init__(self, sql_backend: SqlBackend):
        self._sql_backend = sql_backend

    def get_metadata(self, entity: TableIdentifier) -> TableMetadata:
        schema_query = build_metadata_query(entity)
        query_result: Iterator[Row] = self._sql_backend.fetch(schema_query)
        columns = {
            ColumnMetadata(str(row["col_name"]), str(row["data_type"]))
            for row in query_result
            if not str(row["col_name"]).startswith("#")
        }
        return TableMetadata(entity, sorted(columns, key=lambda x: x.name))
