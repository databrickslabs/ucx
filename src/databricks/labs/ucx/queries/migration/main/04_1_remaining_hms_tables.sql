/* --title 'List of remaining tables in HMS' --type table --width 6 */
SELECT
  SUBSTRING(message, LENGTH('remained-hive-metastore-table: ') + 1) AS message
FROM inventory.logs
WHERE
  message LIKE 'remained-hive-metastore-table: %'