-- viz type=table, name=Clusters, columns=failure,cluster_name,cluster_id,creator
-- widget title=Incompatible clusters, row=6, col=0, size_x=3, size_y=8
SELECT EXPLODE(FROM_JSON(failures, 'array<string>')) AS failure,
    cluster_id,
    cluster_name,
    creator
FROM $inventory.clusters
WHERE NOT STARTSWITH(cluster_name, "job-")
ORDER BY cluster_id DESC