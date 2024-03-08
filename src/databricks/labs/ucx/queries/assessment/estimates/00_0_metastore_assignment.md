-- widget title=Metastore assignment, row=0, col=0, size_x=2, size_y=8

## 1 - Metastore assignment

The first step of adopting is UC is attaching your current workspace to a UC metastore.

This section automatically detects if your workspace has been attached to a UC metastore, and also detects jobs that can potentially fail when attaching the workspace to the metastore.

Follow the docs below to attach your workspace to the metastore:

[[AWS]](https://docs.databricks.com/en/data-governance/unity-catalog/enable-workspaces.html)  
[[Azure]](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/enable-workspaces)  
[[GCP]](https://docs.gcp.databricks.com/data-governance/unity-catalog/enable-workspaces.html)

If any incompatible submit runs has been detected, please follow the steps highlighted below:

1. Find out the incompatible jobs in your local orchestrator based on the object_id identified by UCX
2. Change the job configuration to include the following in the ClusterInfo:   “data_security_mode”: “NONE”
3. Alternatively:
    1. Create Cluster Policy for External Orchestrators and set “data_security_mode”: “NONE”
    2. Assign Cluster Policy to Service Principals