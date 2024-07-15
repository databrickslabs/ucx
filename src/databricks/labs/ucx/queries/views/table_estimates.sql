SELECT
  CONCAT(catalog, '.', database, '.', name) AS table_name,
  object_type,
  table_format,
  CASE
    WHEN object_type = 'MANAGED' AND table_format = 'DELTA'
    THEN 0.5 /* CTAS or recreate as external table, then SYNC */
    WHEN object_type = 'MANAGED' AND table_format <> 'DELTA'
    THEN 2 /* Can vary depending of format */
    WHEN object_type = 'EXTERNAL' AND table_format = 'DELTA' AND STARTSWITH(location, 'dbfs:/')
    THEN 0.5 /* Must CTAS the target table */
    WHEN object_type = 'EXTERNAL' AND table_format = 'DELTA' AND STARTSWITH(location, 'wasbs:/')
    THEN 1 /* Must Offload data to abfss */
    WHEN object_type = 'EXTERNAL' AND table_format = 'DELTA' AND STARTSWITH(location, 'adl:/')
    THEN 1 /* Must Offload data to abfss */
    WHEN object_type = 'EXTERNAL' AND table_format = 'DELTA'
    THEN 0.1 /* In place SYNC, mostly quick */
    WHEN object_type = 'EXTERNAL' AND table_format IN ('SQLSERVER', 'MYSQL', 'SNOWFLAKE')
    THEN 2 /* Must uses Lakehouse Federation */
    WHEN object_type = 'EXTERNAL' AND table_format <> 'DELTA'
    THEN 1 /* Can vary depending of format */
    WHEN object_type = 'VIEW'
    THEN 2 /* Can vary depending of view complexity and number of tables used in the view */
    ELSE NULL
  END AS estimated_hours
FROM $inventory.tables
WHERE
  NOT STARTSWITH(name, '__apply_changes')