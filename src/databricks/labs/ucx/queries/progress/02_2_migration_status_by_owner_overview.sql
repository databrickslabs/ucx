/* --title 'Tables and views migration completion (%)' --description 'Per owner' */
WITH migration_statuses AS (
    SELECT *
    FROM inventory.historical
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
