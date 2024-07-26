/* --title 'Table counts by storage' --width 1 --height 3 */
SELECT
  storage,
  COUNT(*) AS count
FROM (
  SELECT
    CASE
      WHEN STARTSWITH(location, 'dbfs:/mnt')
      THEN 'DBFS MOUNT'
      WHEN STARTSWITH(location, '/dbfs/mnt')
      THEN 'DBFS MOUNT'
      WHEN STARTSWITH(location, 'dbfs:/databricks-datasets')
      THEN 'Databricks Demo Dataset'
      WHEN STARTSWITH(location, '/dbfs/databricks-datasets')
      THEN 'Databricks Demo Dataset'
      WHEN STARTSWITH(location, 'dbfs:/')
      THEN 'DBFS ROOT'
      WHEN STARTSWITH(location, '/dbfs/')
      THEN 'DBFS ROOT'
      WHEN STARTSWITH(location, 'wasb')
      THEN 'UNSUPPORTED'
      WHEN STARTSWITH(location, 'adl')
      THEN 'UNSUPPORTED'
      ELSE 'EXTERNAL'
    END AS storage
  FROM inventory.tables
)
GROUP BY
  storage
ORDER BY
  storage