-- viz type=table, name=Clusters, columns=cluster_id,cluster_name,creator,compatible,failures
-- widget title=Clusters, col=0, row=25, size_x=6, size_y=8
SELECT cluster_id,
       cluster_name,
       creator,
       IF(success = 1, "Compatible", "Incompatible") AS compatible,
       failures
FROM $inventory.clusters
WHERE NOT STARTSWITH(cluster_name, "job-")