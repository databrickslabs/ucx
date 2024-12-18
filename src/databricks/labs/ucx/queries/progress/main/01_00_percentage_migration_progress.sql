/* --title 'Overall progress (%)' --width 2 */
SELECT
    ROUND(100 * try_divide(COUNT_IF(SIZE(failures) = 0), COUNT(*)), 2) AS percentage
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type IN ('ClusterInfo', 'Grant', 'Dashboard', 'JobInfo', 'PipelineInfo', 'PolicyInfo', 'Table', 'Udf')
