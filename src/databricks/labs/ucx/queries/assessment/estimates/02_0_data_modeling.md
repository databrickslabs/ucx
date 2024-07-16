---
height: 10
width: 2
---

## 3 - UC Data modeling

The third step of a successful UC migration is defining your target data model on UC.
This step is required in order to choose in which catalogs the existing data in Hive Metastore will land.

As a starting point, consider creating a catalog that has the same name as your workspace.
For example, a table `database.table1` will land in the `workspace_name.database.table1` table.

The complexity factor is relative to the number of databases and tables identified during the assessment.
