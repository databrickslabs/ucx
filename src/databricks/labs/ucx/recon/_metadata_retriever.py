import re
from collections.abc import Iterator

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.core import Row

from ._base import TableIdentifier, TableMetadata, ColumnMetadata, TableMetadataRetriever


class DatabricksTableMetadataRetriever(TableMetadataRetriever):
    def __init__(self, sql_backend: SqlBackend):
        self._sql_backend = sql_backend

    def get_metadata(self, entity: TableIdentifier) -> TableMetadata:
        schema_query = _prepare_metadata_query(entity)
        query_result: Iterator[Row] = self._sql_backend.fetch(schema_query)
        columns = {
            ColumnMetadata(str(row["col_name"]), str(row["data_type"]))
            for row in query_result
            if not str(row["col_name"]).startswith("#")
        }
        return TableMetadata(entity, sorted(columns, key=lambda x: x.name))


def _prepare_metadata_query(entity: TableIdentifier) -> str:
    if entity.catalog == "hive_metastore":
        return f"DESCRIBE TABLE {entity.fqn}"

    query = f"""
        SELECT 
            LOWER(column_name) AS col_name, 
            full_data_type AS data_type
        FROM 
            {entity.catalog}.information_schema.columns
        WHERE
            LOWER(table_catalog)='{entity.catalog}' AND 
            LOWER(table_schema)='{entity.schema}' AND 
            LOWER(table_name) ='{entity.table}'
        ORDER BY col_name"""

    return re.sub(r'\s+', ' ', query)
