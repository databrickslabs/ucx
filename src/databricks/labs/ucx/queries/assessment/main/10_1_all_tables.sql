/* --title 'Table Types' --filter name --width 6 */
SELECT
  CONCAT(tables.`database`, '.', tables.name) AS name,
  object_type AS type,
  table_format AS format,
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
  END AS storage,
  IF(table_format = 'DELTA', 'Yes', 'No') AS is_delta,
  location,
  CASE
    WHEN size_in_bytes IS NULL
    THEN 'Non DBFS Root'
    WHEN size_in_bytes > 10000000000000000
    THEN 'SIZE OUT OF RANGE'
    WHEN size_in_bytes < 100
    THEN CONCAT(CAST(size_in_bytes AS STRING), ' Bytes')
    WHEN size_in_bytes < 100000
    THEN CONCAT(CAST(ROUND(size_in_bytes / 1024, 2) AS STRING), 'KB')
    WHEN size_in_bytes < 100000000
    THEN CONCAT(CAST(ROUND(size_in_bytes / 1024 / 1024, 2) AS STRING), 'MB')
    WHEN size_in_bytes < 100000000000
    THEN CONCAT(CAST(ROUND(size_in_bytes / 1024 / 1024 / 1024, 2) AS STRING), 'GB')
    ELSE CONCAT(CAST(ROUND(size_in_bytes / 1024 / 1024 / 1024 / 1024, 2) AS STRING), 'TB')
  END AS table_size
FROM inventory.tables AS tables
LEFT OUTER JOIN inventory.table_size AS table_size
  ON tables.catalog = table_size.catalog AND tables.database = table_size.database AND tables.name = table_size.name