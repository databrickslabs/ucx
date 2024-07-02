from collections.abc import Iterator

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row

from .base import TableIdentifier, TableMetadata, ColumnMetadata, TableMetadataRetriever


class DatabricksTableMetadataRetriever(TableMetadataRetriever):
    def __init__(self, sql_backend: SqlBackend):
        self._sql_backend = sql_backend

    def get_metadata(self, entity: TableIdentifier) -> TableMetadata:
        """
        This method retrieves the metadata for a given table. It takes a TableIdentifier object as input,
        which represents the table for which the metadata is to be retrieved.

        Note: This method does not handle exceptions raised during the execution of the SQL query. These exceptions are
        expected to be handled by the caller in a manner appropriate for their context.
        """
        schema_query = self._build_metadata_query(entity)
        query_result: Iterator[Row] = self._sql_backend.fetch(schema_query)
        # The code uses a set comprehension to automatically deduplicate the column metadata entries,
        # Partition information are typically prefixed with a # symbol,
        # so any column name starting with # is excluded from the final set of column metadata.
        # The column metadata objects are sorted by column name to ensure a consistent order.
        columns = {
            ColumnMetadata(str(row["col_name"]), str(row["data_type"]))
            for row in query_result
            if not str(row["col_name"]).startswith("#")
        }
        return TableMetadata(entity, sorted(columns, key=lambda x: x.name))

    @classmethod
    def _build_metadata_query(cls, entity: TableIdentifier) -> str:
        if entity.catalog == "hive_metastore":
            return f"DESCRIBE TABLE {entity.fqn_escaped}"

        query = f"""
            SELECT
                LOWER(column_name) AS col_name,
                full_data_type AS data_type
            FROM
                {entity.catalog_escaped}.information_schema.columns
            WHERE
                LOWER(table_catalog)='{entity.catalog}' AND
                LOWER(table_schema)='{entity.schema}' AND
                LOWER(table_name) ='{entity.table}'
            ORDER BY col_name"""

        return query
