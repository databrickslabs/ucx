from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.recon.base import TableIdentifier, DataProfilingResult, TableMetadata, ColumnMetadata
from databricks.labs.ucx.recon.data_profiler import StandardDataProfiler
from databricks.labs.ucx.recon.metadata_retriever import DatabricksTableMetadataRetriever


def test_data_profiling(metadata_row_factory, row_count_row_factory):
    table_identifier = TableIdentifier("catalog1", "db1", "table1")
    sql_backend = MockBackend(
        rows={
            f"{table_identifier.catalog}\\.information_schema\\.columns": metadata_row_factory[
                ("col2", "string"),
                ("col1", "int"),
                ("col3", "array<string>"),
            ],
            r"SELECT COUNT\(\*\) as row_count FROM": row_count_row_factory[100,],
        }
    )

    expected_data_profile_info = DataProfilingResult(
        row_count=100,
        table_metadata=TableMetadata(
            identifier=table_identifier,
            columns=[
                ColumnMetadata(name="col1", data_type="int"),
                ColumnMetadata(name="col2", data_type="string"),
                ColumnMetadata(name="col3", data_type="array<string>"),
            ],
        ),
    )

    metadata_retriever = DatabricksTableMetadataRetriever(sql_backend)
    data_profiler = StandardDataProfiler(sql_backend, metadata_retriever)
    actual_data_profile_info = data_profiler.profile_data(table_identifier)
    assert actual_data_profile_info == expected_data_profile_info
