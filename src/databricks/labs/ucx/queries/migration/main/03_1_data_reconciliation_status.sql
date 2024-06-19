-- --title 'Migration status'
-- widget title=Migration status, row=3, col=0, size_x=8, size_y=16
SELECT
  concat_ws('.', tables.`catalog`, tables.`database`, tables.name) AS table_name,
  tables.object_type,
  tables.table_format,
  tables.location,
  CASE
      WHEN migration_status.dst_table IS NOT NULL THEN 'Yes'
      ELSE 'No'
  END AS upgraded_status,
  concat_ws('.', migration_status.dst_catalog, migration_status.dst_schema, migration_status.dst_table) AS upgraded_to_table_name,
  reconciliation_results.schema_matches,
  reconciliation_results.column_comparison,
  reconciliation_results.data_matches,
  reconciliation_results.source_row_count,
  reconciliation_results.target_row_count,
  reconciliation_results.source_missing_count,
  reconciliation_results.target_missing_count,
  reconciliation_results.error_message as reconciliation_error,
  tables.view_text,
  tables.storage_properties,
  tables.is_partitioned
FROM
    inventory.tables AS tables
  LEFT JOIN
    inventory.migration_status AS migration_status
  ON tables.`database` = migration_status.src_schema AND tables.name = migration_status.src_table
  LEFT JOIN
    inventory.reconciliation_results AS reconciliation_results
  ON tables.`database` = reconciliation_results.src_schema AND tables.name = reconciliation_results.src_table
