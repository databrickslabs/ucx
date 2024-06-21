---
height: 7
---

## Compute Access Mode Limitations

Unity Catalog enabled Shared Compute enforces strict user isolation and many of the SparkContext and Distributed ML routines are thus not allowed. The best alternative will be to run these workloads on Assigned clusters. See [Databricks documentation for more information](https://docs.databricks.com/en/compute/access-mode-limitations.html#compute-access-mode-limitations)

The analysis offered in this notebook performs pattern matching against the last 90 days of audit logs. Simple pattern matching can provide good awareness of findings, though note many edge cases and obfuscations may be missed.

### Operating Requirements
To use this report:
- Unity Catalog must be [enabled](https://docs.databricks.com/en/administration-guide/index.html#enable-unity-catalog)
- This workspace must be [attached to a Unity Catalog metastore](https://docs.databricks.com/en/data-governance/unity-catalog/create-metastore.html#step-3-create-the-metastore-and-attach-a-workspace)
- The `system` catalog has been [bound to this workspace (default)](https://docs.databricks.com/en/data-governance/unity-catalog/create-catalogs.html#bind-a-catalog-to-one-or-more-workspaces)
- `system.access` and `system.compute` [system table schemas have been enabled](https://docs.databricks.com/en/administration-guide/system-tables/index.html#enable-system-table-schemas)
- This user has been [granted access to the system catalog and schemas](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html#unity-catalog-privileges-and-securable-objects)


### Compute Access Mode Limitation Summary
This widget will display a summary of the findings, the # workspaces, notebooks, clusters and users potentially impacted by [compute access mode limitations](https://docs.databricks.com/en/compute/access-mode-limitations.html#compute-access-mode-limitations)
