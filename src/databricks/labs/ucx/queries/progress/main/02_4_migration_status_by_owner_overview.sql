/* --title 'Overview' --description 'Tables and views migration' --width 5 */
WITH migration_statuses AS (
    SELECT *
    FROM ucx_catalog.multiworkspace.latest_historical_per_workspace
    WHERE object_type = "migration_status"
)

SELECT
    owner,
    100 * COUNT_IF(SIZE(failures) = 0) / SUM(COUNT(*)) OVER (partition by owner) AS percentage,
    COUNT(*) AS total,
    COUNT_IF(SIZE(failures) = 0) AS total_migrated,
    COUNT_IF(SIZE(failures) > 0) AS total_not_migrated
FROM migration_statuses
GROUP BY owner
