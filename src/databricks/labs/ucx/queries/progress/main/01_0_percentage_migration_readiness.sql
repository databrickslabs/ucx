/* --title 'Overall readiness (%)' --width 2 */
SELECT
    100 * COUNT_IF(size(failures) = 0) / COUNT(*) AS percentage
FROM ucx_catalog.multiworkspace.latest_historical_per_workspace
WHERE object_type IN ('udfs', 'grants', 'jobs', 'clusters', 'tables', 'pipelines', 'policies')
