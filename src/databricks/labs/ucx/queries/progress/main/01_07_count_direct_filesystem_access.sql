/* --title 'Direct filesystem access progress (#)' --description 'Unsupported in Unity Catalog' */
SELECT COUNT(*) AS counter
FROM ucx_catalog.multiworkspace.objects_snapshot
WHERE object_type = "DirectFsAccess"
    -- Redundant filter as a direct filesystem access is a failure by definition (see description above),
    -- however, filter is defined for explicitness and as this knowledge is not "known" to this query.
    AND SIZE(failures) > 0
