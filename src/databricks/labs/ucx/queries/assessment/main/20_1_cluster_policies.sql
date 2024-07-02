-- --title 'Cluster Policies' --width 3 --height 6
SELECT
    distinct policy_name,
    cluster.spark_version as cluster_dbr_version,
    policy.spark_version as policy_spark_version
FROM inventory.clusters as cluster
JOIN inventory.policies as policy
ON cluster.policy_id=policy.policy_id
