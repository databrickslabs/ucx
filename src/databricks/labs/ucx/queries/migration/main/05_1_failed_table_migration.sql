/* --title 'List of migration failures' --type table --width 6 */
SELECT
  SUBSTRING(message, LENGTH('failed-to-migrate: ') + 1) AS message
FROM inventory.logs
WHERE
  message LIKE 'failed-to-migrate:%'