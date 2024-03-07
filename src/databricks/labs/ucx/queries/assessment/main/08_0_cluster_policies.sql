-- viz type=table, name=Cluster Policies, columns=policy_name,dbr_version
-- widget title=Cluster Policies, row=8, col=0, size_x=3, size_y=8
SELECT
    distinct policy_name,
    spark_version as dbr_version
FROM $inventory.clusters as cluster
JOIN $inventory.policies as policy
ON cluster.policy_id=policy.policy_id