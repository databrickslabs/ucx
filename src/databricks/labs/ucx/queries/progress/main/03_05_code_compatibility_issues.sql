/* --title 'Code compatability issues' --width 6 */
SELECT object_type, explode(failures) AS failure
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = 'QueryProblem'
