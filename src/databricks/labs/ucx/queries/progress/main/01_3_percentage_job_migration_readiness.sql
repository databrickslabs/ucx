/* --title 'Job migration readiness (%)' */
SELECT
    ROUND(100 * TRY_DIVIDE(COUNT_IF(size(failures) = 0), COUNT(*)), 2) AS percentage
FROM ucx_catalog.multiworkspace.latest_historical_per_workspace
WHERE object_type = "jobs"
