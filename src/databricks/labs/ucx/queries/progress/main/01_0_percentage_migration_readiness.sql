/* --title 'Overall readiness (%)' --width 2 */
SELECT
    ROUND(100 * try_divide(COUNT_IF(size(failures) = 0), COUNT(*)), 2) AS percentage
FROM ucx_catalog.multiworkspace.latest_historical_per_workspace
WHERE object_type IN ('udfs', 'grants', 'jobs', 'clusters', 'tables', 'pipelines', 'policies')
