with flattened as (
  select
    *,
    from_json(
      schema_comparison,
      'STRUCT<data: ARRAY<STRUCT<is_matching: BOOLEAN, notes: STRING, source_column: STRING, source_datatype: STRING, target_column: STRING, target_datatype: STRING>>, is_matching: BOOLEAN>'
    ) as schema_comparison_result,
    from_json(
      data_comparison,
      'STRUCT<num_missing_records_in_source: BIGINT, num_missing_records_in_target: BIGINT, source_row_count: BIGINT, target_row_count: BIGINT>'
    ) as data_comparison_result
  from
    $inventory.recon_result
)
select
  concat_ws('.', src_schema, src_table) AS source_table,
  concat_ws('.', dst_catalog, dst_schema, dst_table) AS target_table,
  schema_matches,
  data_matches,
  data_comparison_result.source_row_count,
  data_comparison_result.target_row_count,  
  data_comparison_result.num_missing_records_in_source,
  data_comparison_result.num_missing_records_in_target,
  schema_comparison_result.data as column_comparison
from
  flattened