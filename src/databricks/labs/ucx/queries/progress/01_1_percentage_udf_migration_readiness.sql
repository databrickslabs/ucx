/* --title 'UDF migration readiness (%)' */
SELECT
    100 * COUNT_IF(size(failures) = 0) / COUNT(*) AS percentage
FROM multiworkspace.historical
WHERE object_type = "udfs"
