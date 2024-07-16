/* --title 'Migration status' --width 6 */
SELECT
  CONCAT_WS('.', tables.`catalog`, tables.`database`, tables.name) AS table_name,
  tables.object_type,
  tables.table_format,
  tables.location,
  CASE WHEN NOT migration_status.dst_table IS NULL THEN 'Yes' ELSE 'No' END AS upgraded_status,
  CONCAT_WS('.', migration_status.dst_catalog, migration_status.dst_schema, migration_status.dst_table) AS upgraded_to_table_name,
  reconciliation_results.schema_matches,
  reconciliation_results.column_comparison,
  reconciliation_results.data_matches,
  reconciliation_results.source_row_count,
  reconciliation_results.target_row_count,
  reconciliation_results.source_missing_count,
  reconciliation_results.target_missing_count,
  reconciliation_results.error_message AS reconciliation_error,
  tables.view_text,
  tables.storage_properties,
  tables.is_partitioned
FROM inventory.tables AS tables
LEFT JOIN inventory.migration_status AS migration_status
  ON tables.`database` = migration_status.src_schema AND tables.name = migration_status.src_table
LEFT JOIN inventory.reconciliation_results AS reconciliation_results
  ON tables.`database` = reconciliation_results.src_schema AND tables.name = reconciliation_results.src_table