-- viz type=table, name=Cluster Policies, columns=policy_name,cluster_dbr_version,policy_spark_version
-- widget title=Cluster Policies, row=22, col=4, size_x=3, size_y=8
SELECT
    distinct policy_name,
    cluster.spark_version as cluster_dbr_version,
    policy.spark_version as policy_spark_version
FROM $inventory.clusters as cluster
JOIN $inventory.policies as policy
ON cluster.policy_id=policy.policy_id