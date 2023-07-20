"""
Permissions to inventorize:

Clusters:
- Clusters +
- Cluster policies +
- Pools +
- Instance Profile (for AWS) +

Workflows:
- Delta Live Tables pipelines ?
- Jobs ?

ML:

- MLflow experiments ?
- MLflow registry

SQL:
- Databricks SQL warehouses +
- Dashboard +
- Query +
- Alerts +

Security:
- Tokens +
- Password (for AWS) ???
- Secrets ?

Workspace:
- Notebooks
- Files
- Repos
- Directories

To be clarified:
- Notebooks -> covered by Workspace API
- Files -> it it's UC files, they're not workspace-level.
If it's about files in the Workspace, it's covered by Workspace API.
- Repos -> covered by workspace API
- Directories -> covered by workspace API

- Table ACL (Non UC Cluster) -
"""
