/* --title 'Data modeling complexity' --height 10 --width 1 */
SELECT
  CASE
    WHEN distinct_tables = 0
    THEN NULL
    WHEN distinct_tables BETWEEN 1 AND 100
    THEN 'S'
    WHEN distinct_tables BETWEEN 101 AND 300
    THEN 'M'
    WHEN distinct_tables > 301
    THEN 'L'
    ELSE NULL
  END AS uc_model_complexity
FROM (
  SELECT
    COUNT(DISTINCT CONCAT(database, '.', name)) AS distinct_tables
  FROM inventory.tables
)