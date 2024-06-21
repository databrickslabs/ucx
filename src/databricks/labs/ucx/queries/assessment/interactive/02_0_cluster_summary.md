---
height: 4
---

## Cluster Summary

Detailed itemized findings by cluster helping users prioritize which clusters to upgrade.
Typical upgrade paths are:
-  Upgrade cluter runtime to latest LTS version (e.g. 14.3 LTS)

-  Migrate users with GPU, Distributed ML, Spark Context, R type workloads to Assigned clusters. Implement cluster policies and pools to even out startup time and limit upper cost boundry

-  Upgrade SQL only users (and BI tools) to SQL Warehouses (much better SQL / Warehouse experience and lower cost)

-  For users with single node python ML requirements, Shared Compute with `%pip install` library support or Personal Compute with pools and compute controls may provide a better experience and better manageability.

- For single node ML users on a crowded driver node of a large shared cluster, will get a better experience with Personal Compute policies combined with (warm) Compute pools
