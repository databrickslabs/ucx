/* --title 'Incompatible clusters' --width 3 --height 6 */
SELECT
  EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
  cluster_id,
  cluster_name,
  creator
FROM inventory.clusters
WHERE
  NOT STARTSWITH(cluster_name, 'job-')
ORDER BY
  cluster_id DESC