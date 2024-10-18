/*
--title 'Tables and views not migrated'
--overrides '{"spec": {
    "version": 3,
    "widgetType": "bar",
    "encodings": {
        "x":{"fieldName": "owner", "scale": {"type": "categorical"}, "displayName": "owner"},
        "y":{"fieldName": "count", "scale": {"type": "quantitative"}, "displayName": "count"}
    }
}}'
*/
WITH migration_statuses AS (
    SELECT *
    FROM inventory.historical
    WHERE object_type = "migration_status"
)

SELECT
    owner,
    COUNT_IF(SIZE(failures) > 0) AS count
FROM migration_statuses
GROUP BY owner
