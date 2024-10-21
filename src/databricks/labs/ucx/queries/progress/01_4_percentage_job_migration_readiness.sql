/*
--title 'Job migration readiness'
--overrides '{"spec": {
    "encodings": {
        "value": {
            "format": {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 2}}
        }
    }
}}'
*/
SELECT
    100 * COUNT_IF(size(failures) = 0) / COUNT(*) AS percentage
FROM multiworkspace.historical
WHERE object_type = "jobs"
