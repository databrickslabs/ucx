---
height: 4
---

# Direct filesystem access problems

The table below assists with verifying if workflows and dashboards require direct filesystem access.
As a reminder, `dbfs:/` is not supported in Unity Catalog, and more generally direct filesystem access is discouraged.
Rather, data should be accessed via Unity tables.

Each row:
- Points to a direct filesystem access detected in the code using the code path, query or workflow & task reference and start/end line & column;
- Provides the _lineage_ i.e. which `workflow -> task -> notebook...` execution sequence leads to that access.

