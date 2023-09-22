-- viz type=table, name=Clusters, columns=cluster_id,cluster_name,creator,compatible,failures
-- widget title=Clusters, col=0, row=15, size_x=3, size_y=4
SELECT
  cluster_id,
  cluster_name,
  creator,
  CASE WHEN success=1 THEN "Compatible" ELSE "Incompatible" END AS compatible,
  failures
FROM $inventory.clusters