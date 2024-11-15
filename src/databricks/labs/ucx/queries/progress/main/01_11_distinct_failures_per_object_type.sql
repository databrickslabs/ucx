/* --title 'Distinct failures per object type' --width 6 */
with failures AS (
  SELECT object_type, explode(failures) AS failure
  FROM ucx_catalog.multiworkspace.objects_snapshot
  WHERE object_type IN ('ClusterInfo', 'DirectFsAccess', 'Grant', 'JobInfo', 'PipelineInfo', 'PolicyInfo', 'QueryProblem', 'Table', 'Udf', 'UsedTable')
)

SELECT
    object_type,
    COUNT(*) AS count,
    failure
FROM failures
GROUP BY object_type, failure
ORDER BY object_type, failure
