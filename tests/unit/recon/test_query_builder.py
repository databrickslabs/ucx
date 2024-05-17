import re

from databricks.labs.ucx.recon.base import TableIdentifier, DataProfilingResult, TableMetadata, ColumnMetadata
from databricks.labs.ucx.recon.query_builder import (
    build_metadata_query,
    build_data_comparison_query,
    DATA_COMPARISON_QUERY_TEMPLATE,
)


def test_hms_metadata_query():
    table_identifier = TableIdentifier("hive_metastore", "db1", "table1")
    actual_query = build_metadata_query(table_identifier)
    expected_query = "DESCRIBE TABLE hive_metastore.db1.table1"
    assert _normalize_string(actual_query) == _normalize_string(expected_query)


def test_unity_metadata_query():
    table_identifier = TableIdentifier("catalog1", "db1", "table1")
    actual_query = build_metadata_query(table_identifier)
    expected_query = """
        SELECT 
            LOWER(column_name) AS col_name, 
            full_data_type AS data_type
        FROM 
            catalog1.information_schema.columns
        WHERE
            LOWER(table_catalog)='catalog1' AND
            LOWER(table_schema)='db1' AND
            LOWER(table_name) ='table1'
        ORDER BY col_name
    """
    assert _normalize_string(actual_query) == _normalize_string(expected_query)


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

    actual_query = build_data_comparison_query(source_data_profile, target_data_profile)
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
    expected_query = DATA_COMPARISON_QUERY_TEMPLATE.format(
        source_hash_expr=f"SHA2(CONCAT_WS('|', {', '.join(source_hash_columns)}), 256)",
        target_hash_expr=f"SHA2(CONCAT_WS('|', {', '.join(target_hash_columns)}), 256)",
        source_table_fqn="hive_metastore.db1.table1",
        target_table_fqn="catalog1.schema1.table2",
    )
    assert _normalize_string(actual_query) == _normalize_string(expected_query)


def _normalize_string(value):
    return re.sub(r'\s+', ' ', value.strip().lower())
