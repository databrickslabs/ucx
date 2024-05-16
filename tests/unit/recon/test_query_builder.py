import re

from databricks.labs.ucx.recon.base import TableIdentifier, DataProfilingResult, TableMetadata, ColumnMetadata
from databricks.labs.ucx.recon.query_builder import build_metadata_query, build_data_comparison_query


def test_hms_metadata_query():
    table_identifier = TableIdentifier("hive_metastore", "db1", "table1")
    actual_query = build_metadata_query(table_identifier)
    expected_query = "DESCRIBE TABLE hive_metastore.db1.table1"
    assert re.sub(r'\s+', ' ', actual_query.strip().lower()) == re.sub(r'\s+', ' ', expected_query.strip().lower())


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
    assert re.sub(r'\s+', ' ', actual_query.strip().lower()) == re.sub(r'\s+', ' ', expected_query.strip().lower())


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
    expected_query = """
        WITH compare_results AS (
            SELECT 
                CASE 
                    WHEN source.hash_value IS NULL AND target.hash_value IS NULL THEN TRUE
                    WHEN source.hash_value IS NULL OR target.hash_value IS NULL THEN FALSE
                    WHEN source.hash_value = target.hash_value THEN TRUE
                    ELSE FALSE
                END AS is_match,
                CASE 
                    WHEN target.hash_value IS NULL THEN 1
                    ELSE 0
                END AS num_missing_records_in_target,
                CASE 
                    WHEN source.hash_value IS NULL THEN 1
                    ELSE 0
                END AS num_missing_records_in_source
            FROM (
                SELECT SHA2(CONCAT_WS('|', 
                        COALESCE(TRIM(col1), ''), 
                        COALESCE(TRIM(TO_JSON(SORT_ARRAY(col2))), ''), 
                        COALESCE(TRIM(TO_JSON(col3)), '')), 256) AS hash_value
                FROM hive_metastore.db1.table1
            ) AS source
            FULL OUTER JOIN (
                SELECT SHA2(CONCAT_WS('|', 
                    COALESCE(TRIM(col1), ''), 
                    COALESCE(TRIM(TO_JSON(SORT_ARRAY(col2))), ''), 
                    COALESCE(TRIM(TO_JSON(col3)), '')), 256) AS hash_value
                FROM catalog1.schema1.table2
            ) AS target
            ON source.hash_value = target.hash_value
        )
        SELECT 
            COUNT(*) AS total_mismatches,
            COALESCE(SUM(num_missing_records_in_target), 0) AS num_missing_records_in_target,
            COALESCE(SUM(num_missing_records_in_source), 0) AS num_missing_records_in_source
        FROM compare_results
        WHERE is_match IS FALSE;
    """

    assert re.sub(r'\s+', ' ', actual_query.strip().lower()) == re.sub(r'\s+', ' ', expected_query.strip().lower())
