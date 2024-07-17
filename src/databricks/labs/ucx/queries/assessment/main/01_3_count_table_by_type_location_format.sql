/* --title 'Table counts by type, location and format' --width 4 --height 4 */
SELECT object_type AS SOURCE_TABLE_TYPE,
table_format AS FORMAT, LOCATION, count(*) AS COUNT
FROM
(SELECT object_type,
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
  END AS LOCATION
  FROM inventory.tables
WHERE object_type NOT IN ('VIEW'))
GROUP BY ALL
ORDER BY SOURCE_TABLE_TYPE
