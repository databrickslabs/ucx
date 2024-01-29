SELECT
    CASE
        WHEN anonymous_function THEN 'ANONYMOUS FUNCTION'
        WHEN any_file THEN 'ANY FILE'
        WHEN view IS NOT NULL THEN 'VIEW'
        WHEN table IS NOT NULL THEN 'TABLE'
        WHEN database IS NOT NULL THEN 'DATABASE'
        WHEN catalog IS NOT NULL THEN 'CATALOG'
        WHEN udf IS NOT NULL THEN 'UDF'
        ELSE 'UNKNOWN'
    END AS object_type,
    CASE
        WHEN anonymous_function THEN NULL
        WHEN any_file THEN NULL
        WHEN view IS NOT NULL THEN CONCAT(catalog, '.', database, '.', view)
        WHEN table IS NOT NULL THEN  CONCAT(catalog, '.', database, '.', table)
        WHEN database IS NOT NULL THEN  CONCAT(catalog, '.', database)
        WHEN catalog IS NOT NULL THEN catalog
        WHEN udf IS NOT NULL THEN CONCAT(catalog, '.', database, '.', udf)
        ELSE 'UNKNOWN'
    END AS object_id,
    action_type,
    CASE
        WHEN principal
            RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        THEN 'service-principal'
        WHEN principal RLIKE '@' THEN 'user'
        ELSE 'group'
    END AS principal_type,
    principal,
    catalog,
    database,
    table,
    udf
FROM $inventory.grants where database != split("$inventory",'[.]')[1]