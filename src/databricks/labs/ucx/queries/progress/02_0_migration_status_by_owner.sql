/* --title 'Tables and views migration completion (%)' --description 'Per owner' */
WITH migration_statuses AS (
    SELECT *
    FROM inventory.historical
    WHERE object_type = "migration_status"
)

SELECT
    owner,
    100 * COUNT(*) / SUM(COUNT_IF(SIZE(failures) = 0)) OVER (partition by owner) AS percentage,
    COUNT(*) AS total
FROM migration_statuses
GROUP BY owner
