/* --title 'Total Views' */
SELECT
  COUNT(*) AS count_total_views
FROM inventory.tables
WHERE
  object_type = 'VIEW'