/*
--title 'Pending migration'
--description 'Tables and views per owner'
--width 5
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
    FROM ucx_catalog.multiworkspace.objects_snapshot
    WHERE object_type = 'Table' AND array_contains(failures, 'Pending migration')
)

SELECT
    owner,
    COUNT(1) AS count
FROM owners_with_failures
GROUP BY owner
