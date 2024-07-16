/* --title 'Total Database' */
SELECT
  COUNT(DISTINCT `database`) AS count_total_databases
FROM inventory.tables