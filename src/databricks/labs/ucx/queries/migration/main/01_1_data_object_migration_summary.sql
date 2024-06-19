-- --title 'Data Migration Progress'
-- widget row=1, col=4, size_x=2, size_y=4
SELECT
  COUNT(
    CASE
      WHEN migration_status.dst_table IS NOT NULL THEN 1
      ELSE NULL
    END
  ) AS migrated,
  count(*) AS total,
  concat(round(migrated / total * 100, 2), '%') AS migrated_rate
FROM
    inventory.tables AS tables
  LEFT JOIN
    inventory.migration_status AS migration_status
  ON tables.`database` = migration_status.src_schema AND tables.name = migration_status.src_table
