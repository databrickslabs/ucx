WITH
    raw as (
        select
            *,
            if(startswith(action_type, 'DENIED_'),
                array('Explicitly DENYing privileges is not supported in UC.'),
                array()
            ) as failures
        -- TODO: Verify that split works
        from inventory.grants where database is null or database <> split("inventory",'[.]')[1]
    )
SELECT
    CASE
        WHEN anonymous_function THEN 'ANONYMOUS FUNCTION'
        WHEN any_file THEN 'ANY FILE'
        WHEN view IS NOT NULL THEN 'VIEW'
        WHEN table IS NOT NULL THEN 'TABLE'
        WHEN udf IS NOT NULL THEN 'UDF'
        WHEN database IS NOT NULL THEN 'DATABASE'
        WHEN catalog IS NOT NULL THEN 'CATALOG'
        ELSE 'UNKNOWN'
    END AS object_type,
    CASE
        WHEN anonymous_function THEN NULL
        WHEN any_file THEN NULL
        WHEN view IS NOT NULL THEN CONCAT(catalog, '.', database, '.', view)
        WHEN table IS NOT NULL THEN  CONCAT(catalog, '.', database, '.', table)
        WHEN udf IS NOT NULL THEN CONCAT(catalog, '.', database, '.', udf)
        WHEN database IS NOT NULL THEN  CONCAT(catalog, '.', database)
        WHEN catalog IS NOT NULL THEN catalog
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
    view,
    udf,
    any_file,
    anonymous_function,
    if(size(failures) < 1, 1, 0) as success,
    to_json(failures) as failures
FROM raw
