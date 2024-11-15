/*
--title 'Pending migration'
--description 'Tables, views and dfsa per owner'
--width 5
--overrides '{"spec": {
    "version": 3,
    "widgetType": "bar",
    "encodings": {
        "x": {"fieldName": "owner", "scale": {"type": "categorical"}, "displayName": "owner"},
        "y": {"fieldName": "count", "scale": {"type": "quantitative"}, "displayName": "count"}
    }
}}'
*/
WITH owners_with_failures AS (
    SELECT owner
    FROM ucx_catalog.multiworkspace.objects_snapshot
    WHERE object_type IN ('DirectFsAccess', 'UsedTable') AND SIZE(failures) > 0
)

SELECT
    owner,
    COUNT(1) AS count
FROM owners_with_failures
GROUP BY owner
