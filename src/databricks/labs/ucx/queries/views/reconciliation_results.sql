WITH flattened AS (
  SELECT
    *,
    FROM_JSON(
      schema_comparison,
      'STRUCT<data: ARRAY<STRUCT<is_matching: BOOLEAN, notes: STRING, source_column: STRING, source_datatype: STRING, target_column: STRING, target_datatype: STRING>>, is_matching: BOOLEAN>'
    ) AS schema_comparison_result,
    FROM_JSON(
      data_comparison,
      'STRUCT<source_missing_count: BIGINT, target_missing_count: BIGINT, source_row_count: BIGINT, target_row_count: BIGINT>'
    ) AS data_comparison_result
  FROM $inventory.recon_results
)
SELECT
  src_schema,
  src_table,
  dst_catalog,
  dst_schema,
  dst_table,
  schema_matches,
  data_matches,
  data_comparison_result.source_row_count,
  data_comparison_result.target_row_count,
  data_comparison_result.source_missing_count,
  data_comparison_result.target_missing_count,
  schema_comparison_result.data AS column_comparison,
  error_message
FROM flattened