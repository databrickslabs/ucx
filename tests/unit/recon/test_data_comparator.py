import re

from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.recon.base import (
    TableIdentifier,
    DataComparisonResult,
    DataProfilingResult,
    TableMetadata,
    ColumnMetadata,
)
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
            f"SELECT COUNT\\(\\*\\) as row_count FROM {source.fqn_escaped}": row_count_row_factory[100,],
            f"SELECT COUNT\\(\\*\\) as row_count FROM {target.fqn_escaped}": row_count_row_factory[2,],
            "WITH compare_results": data_comp_row_factory[(102, 100, 2),],
        }
    )

    expected_comparison_result = DataComparisonResult(
        source_row_count=100,
        target_row_count=2,
        source_missing_count=2,
        target_missing_count=100,
    )

    data_profiler = StandardDataProfiler(sql_backend, DatabricksTableMetadataRetriever(sql_backend))
    data_comparator = StandardDataComparator(sql_backend, data_profiler)
    actual_comparison_result = data_comparator.compare_data(source, target, True)

    assert actual_comparison_result == expected_comparison_result


def test_prepare_data_comparison_query():
    source = TableIdentifier("hive_metastore", "db1", "table1")
    target = TableIdentifier("catalog1", "schema1", "table2")

    source_data_profile = DataProfilingResult(
        10,
        TableMetadata(
            source,
            [
                ColumnMetadata("col1", "string"),
                ColumnMetadata("col2", "array<string>"),
                ColumnMetadata("col3", "struct<a:int,b:int,c:array<string>>"),
            ],
        ),
    )
    target_data_profile = DataProfilingResult(
        10,
        TableMetadata(
            target,
            [
                ColumnMetadata("col1", "string"),
                ColumnMetadata("col2", "array<string>"),
                ColumnMetadata("col3", "struct<a:int,b:int,c:array<string>>"),
            ],
        ),
    )

    actual_query = (
        StandardDataComparator._build_data_comparison_query(
            source_data_profile,
            target_data_profile,
        )
        .strip()
        .lower()
    )

    source_hash_columns = [
        "COALESCE(TRIM(col1), '')",
        "COALESCE(TRIM(TO_JSON(SORT_ARRAY(col2))), '')",
        "COALESCE(TRIM(TO_JSON(col3)), '')",
    ]
    target_hash_columns = [
        "COALESCE(TRIM(col1), '')",
        "COALESCE(TRIM(TO_JSON(SORT_ARRAY(col2))), '')",
        "COALESCE(TRIM(TO_JSON(col3)), '')",
    ]

    expected_query = (
        StandardDataComparator._DATA_COMPARISON_QUERY_TEMPLATE.format(
            source_hash_expr=f"SHA2(CONCAT_WS('|', {', '.join(source_hash_columns)}), 256)",
            target_hash_expr=f"SHA2(CONCAT_WS('|', {', '.join(target_hash_columns)}), 256)",
            source_table_fqn="`hive_metastore`.`db1`.`table1`",
            target_table_fqn="`catalog1`.`schema1`.`table2`",
        )
        .strip()
        .lower()
    )

    assert re.sub(r'\s+', ' ', actual_query) == re.sub(r'\s+', ' ', expected_query)
