from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.recon.base import TableIdentifier, DataComparisonResult
from databricks.labs.ucx.recon.data_comparator import StandardDataComparator
from databricks.labs.ucx.recon.data_profiler import StandardDataProfiler
from databricks.labs.ucx.recon.metadata_retriever import DatabricksTableMetadataRetriever


def test_data_comparison(metadata_row_factory, row_count_row_factory, data_comp_row_factory):
    source = TableIdentifier("catalog1", "default", "recon_test_table_1")
    target = TableIdentifier("catalog2", "default", "recon_test_table_2")
    sql_backend = MockBackend(
        rows={
            f"{source.catalog}\\.information_schema\\.columns": metadata_row_factory[
                ("col1", "int"),
                ("col2", "string"),
            ],
            f"{target.catalog}\\.information_schema\\.columns": metadata_row_factory[
                ("col1", "int"),
                ("col2", "string"),
            ],
            f"SELECT COUNT\\(\\*\\) as row_count FROM {source.table}": row_count_row_factory[100,],
            f"SELECT COUNT\\(\\*\\) as row_count FROM {target.table}": row_count_row_factory[2,],
            "WITH compare_results": data_comp_row_factory[(102, 100, 2),],
        }
    )

    expected_comparison_result = DataComparisonResult(
        source_row_count=100,
        target_row_count=2,
        num_missing_records_in_target=100,
        num_missing_records_in_source=2,
    )

    data_profiler = StandardDataProfiler(sql_backend, DatabricksTableMetadataRetriever(sql_backend))
    data_comparator = StandardDataComparator(sql_backend, data_profiler)
    actual_comparison_result = data_comparator.compare_data(source, target)

    assert actual_comparison_result == expected_comparison_result
