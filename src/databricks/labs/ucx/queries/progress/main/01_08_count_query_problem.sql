/* --title 'Query problem progress (#)' */
SELECT COUNT(*) AS counter
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = "QueryProblem"
    -- Redundant filter as a query problem is a failure by definition, however, filter is defined for explicitness
    AND SIZE(failures) > 0
