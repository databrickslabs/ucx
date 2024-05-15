import re
from collections.abc import Iterator

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row

from databricks.labs.ucx.recon import TableDescriptor, TableMetadata, ColumnMetadata
from databricks.labs.ucx.recon import TableMetadataRetriever


class DatabricksTableMetadataRetriever(TableMetadataRetriever):
    def __init__(self, sql_backend: SqlBackend):
        self._sql_backend = sql_backend

    def get_metadata(self, source: TableDescriptor) -> TableMetadata:
        schema_query = _prepare_metadata_query(source)
        query_result: Iterator[Row] = self._sql_backend.fetch(schema_query)
        columns = {
            ColumnMetadata(str(row["col_name"]), str(row["data_type"]))
            for row in query_result
            if not str(row["col_name"]).startswith("#")
        }
        return TableMetadata(source, list(columns))


def _prepare_metadata_query(source: TableDescriptor) -> str:
    if source.catalog == "hive_metastore":
        return f"describe table {source.schema}.{source.table}"

    query = f"""
        SELECT 
            LOWER(column_name) AS col_name, 
            full_data_type AS data_type
        FROM 
            {source.catalog}.information_schema.columns
        WHERE
            LOWER(table_catalog)='{source.catalog}' AND 
            LOWER(table_schema)='{source.schema}' AND 
            LOWER(table_name) ='{source.table}'
        ORDER BY col_name"""

    return re.sub(r'\s+', ' ', query)
