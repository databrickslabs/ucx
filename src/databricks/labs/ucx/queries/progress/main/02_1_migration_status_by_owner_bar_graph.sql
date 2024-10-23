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
WITH owners_with_failures AS (
    SELECT owner
    FROM ucx_catalog.multiworkspace.latest_historical_per_workspace
    WHERE object_type = "migration_status" AND SIZE(failures) > 0
)

SELECT
    owner,
    COUNT(1) AS count
FROM owners_with_failures
GROUP BY owner
