/* --title 'Overview' --description 'Table, view and dfsa migration' --width 5 */
WITH migration_statuses AS (
    SELECT owner, object_type, failures
    FROM ucx_catalog.multiworkspace.objects_snapshot
    WHERE object_type IN ('DirectFsAccess', 'UsedTable')
)

SELECT
    owner,
    CASE
        WHEN object_type = 'DirectFsAccess' THEN 'Direct filesystem access'
        WHEN object_type = 'UsedTable' THEN 'Table or view reference'
        ELSE object_type
    END AS object_type,
    DOUBLE(CEIL(100 * COUNT_IF(SIZE(failures) = 0) / SUM(COUNT(*)) OVER (PARTITION BY owner), 2)) AS percentage,
    COUNT(*) AS total,
    COUNT_IF(SIZE(failures) = 0) AS total_migrated,
    COUNT_IF(SIZE(failures) > 0) AS total_not_migrated
FROM migration_statuses
GROUP BY owner, object_type
