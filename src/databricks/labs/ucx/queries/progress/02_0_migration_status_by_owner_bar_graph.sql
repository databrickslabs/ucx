/* --title 'Tables and views not migrated' */
WITH migration_statuses AS (
    SELECT *
    FROM inventory.historical
    WHERE object_type = "migration_status"
)

SELECT
    owner,
    COUNT_IF(SIZE(failures) > 0) AS total_not_migrated
FROM migration_statuses
GROUP BY owner
