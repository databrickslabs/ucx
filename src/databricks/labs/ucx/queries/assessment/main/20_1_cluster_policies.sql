/* --title 'Cluster Policies' --width 3 --height 6 */
SELECT DISTINCT
  policy_name,
  cluster.spark_version AS cluster_dbr_version,
  policy.spark_version AS policy_spark_version
FROM inventory.clusters AS cluster
JOIN inventory.policies AS policy
  ON cluster.policy_id = policy.policy_id