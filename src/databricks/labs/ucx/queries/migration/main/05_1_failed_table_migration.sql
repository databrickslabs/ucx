/* --title 'List of migration failures' --type table --width 6 */
SELECT
  message
FROM inventory.logs
WHERE
  message LIKE 'Failed to migrate %'
