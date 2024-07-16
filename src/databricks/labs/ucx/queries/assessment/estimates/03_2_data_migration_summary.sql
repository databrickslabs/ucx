/* --title 'Table estimates' --height 10 --width 3 */
SELECT
  table_name,
  object_type,
  table_format,
  estimated_hours
FROM inventory.table_estimates