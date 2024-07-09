# ucx[to-json-in-shared-clusters:+1:6:+1:80] toJson() is not available on UC Shared Clusters. Use toSafeJson() on DBR 13.3 LTS or above to get a subset of command context information.
print(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
dbutils.notebook.entry_point.getDbutils().notebook().getContext().toSafeJson()
notebook = dbutils.notebook.entry_point.getDbutils().notebook()
# ucx[to-json-in-shared-clusters:+1:0:+1:30] toJson() is not available on UC Shared Clusters. Use toSafeJson() on DBR 13.3 LTS or above to get a subset of command context information.
notebook.getContext().toJson()
