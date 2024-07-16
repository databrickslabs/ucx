/* --title 'Data Migration Progress' --width 6 */
SELECT
  COUNT(CASE WHEN NOT migration_status.dst_table IS NULL THEN 1 ELSE NULL END) AS migrated,
  COUNT(*) AS total,
  CONCAT(ROUND(TRY_DIVIDE(migrated, total) * 100, 2), '%') AS migrated_rate
FROM inventory.tables AS tables
LEFT JOIN inventory.migration_status AS migration_status
  ON tables.`database` = migration_status.src_schema AND tables.name = migration_status.src_table