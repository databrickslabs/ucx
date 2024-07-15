/* --title 'Group migration complexity' --height 10 --width 1 */
SELECT
  CASE
    WHEN total_groups = 0
    THEN NULL
    WHEN total_groups BETWEEN 1 AND 50
    THEN 'S'
    WHEN total_groups BETWEEN 51 AND 200
    THEN 'M'
    WHEN total_groups > 201
    THEN 'L'
    ELSE NULL
  END AS group_migration_complexity
FROM (
  SELECT
    COUNT(*) AS total_groups
  FROM inventory.groups
)