/* --title 'List of remaining tables in HMS' --width 6 */
SELECT message
FROM inventory.logs
WHERE message LIKE 'remained-hive-metastore-table: %'
