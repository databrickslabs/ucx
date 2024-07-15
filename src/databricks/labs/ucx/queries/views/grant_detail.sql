WITH raw AS (
  SELECT
    *,
    IF(STARTSWITH(action_type, 'DENIED_'), ARRAY('Explicitly DENYing privileges is not supported in UC.'), ARRAY()) AS failures
  FROM $inventory.grants
  WHERE
    database IS NULL OR database <> SPLIT('$inventory', '[.]')[1]
)
SELECT
  CASE
    WHEN anonymous_function
    THEN 'ANONYMOUS FUNCTION'
    WHEN any_file
    THEN 'ANY FILE'
    WHEN NOT view IS NULL
    THEN 'VIEW'
    WHEN NOT table IS NULL
    THEN 'TABLE'
    WHEN NOT udf IS NULL
    THEN 'UDF'
    WHEN NOT database IS NULL
    THEN 'DATABASE'
    WHEN NOT catalog IS NULL
    THEN 'CATALOG'
    ELSE 'UNKNOWN'
  END AS object_type,
  CASE
    WHEN anonymous_function
    THEN NULL
    WHEN any_file
    THEN NULL
    WHEN NOT view IS NULL
    THEN CONCAT(catalog, '.', database, '.', view)
    WHEN NOT table IS NULL
    THEN CONCAT(catalog, '.', database, '.', table)
    WHEN NOT udf IS NULL
    THEN CONCAT(catalog, '.', database, '.', udf)
    WHEN NOT database IS NULL
    THEN CONCAT(catalog, '.', database)
    WHEN NOT catalog IS NULL
    THEN catalog
    ELSE 'UNKNOWN'
  END AS object_id,
  action_type,
  CASE
    WHEN principal RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    THEN 'service-principal'
    WHEN principal RLIKE '@'
    THEN 'user'
    ELSE 'group'
  END AS principal_type,
  principal,
  catalog,
  database,
  table,
  view,
  udf,
  any_file,
  anonymous_function,
  IF(SIZE(failures) < 1, 1, 0) AS success,
  TO_JSON(failures) AS failures
FROM raw