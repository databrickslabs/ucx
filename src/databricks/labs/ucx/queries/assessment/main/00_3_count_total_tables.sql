/* --title 'Total Tables' */
SELECT
  COUNT(*) AS count_total_tables
FROM inventory.tables
WHERE
  object_type != 'VIEW'
