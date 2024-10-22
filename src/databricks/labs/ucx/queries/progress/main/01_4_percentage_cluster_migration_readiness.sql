/* --title 'Cluster migration readiness (%)' */
SELECT
    100 * COUNT_IF(size(failures) = 0) / COUNT(*) AS percentage
FROM ucx_catalog.multiworkspace.historical
WHERE object_type = "clusters"
