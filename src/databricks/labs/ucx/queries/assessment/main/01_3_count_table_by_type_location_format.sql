/* --title 'Table counts by type, location and format' --width 2 --height 3 */
SELECT
  object_type AS source_table_type,
  table_format AS format,
  location,
  COUNT(*) AS count
FROM (
  SELECT
    object_type,
    table_format,
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
    END AS location
  FROM inventory.tables
  WHERE
    NOT object_type IN ('VIEW')
)
GROUP BY ALL
ORDER BY
  source_table_type