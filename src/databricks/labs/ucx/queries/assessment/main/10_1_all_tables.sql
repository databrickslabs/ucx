/*
--title 'Table Types'
--filter name
--width 6
--overrides '{"spec":{
    "encodings":{
      "columns": [
        {"fieldName": "name", "booleanValues": ["false", "true"], "linkUrlTemplate": "/explore/data/hive_metastore/{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}", "linkOpenInNewTab": true, "type": "string", "displayAs": "link", "title": "name"},
        {"fieldName": "type", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "type"},
        {"fieldName": "format", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "format"},
        {"fieldName": "storage", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "storage"},
        {"fieldName": "is_delta", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "is_delta"},
        {"fieldName": "location", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "location"},
        {"fieldName": "table_size", "booleanValues": ["false", "true"], "type": "string", "displayAs": "string", "title": "table_size"}
      ]}
  }}'
*/
SELECT
  CONCAT(tables.`database`, '.', tables.name) AS name,
  object_type AS type,
  UPPER(table_format) AS format,
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
  IF(UPPER(table_format) = 'DELTA', 'Yes', 'No') AS is_delta,
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
ORDER by type, name
