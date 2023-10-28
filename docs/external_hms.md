# External HMS Integration
### TL;DR
The UCX toolkit by default relies on the internal workspace HMS as a source for tables and views.
<br/>Many DB users utilize an external HMS instead of the Workspace HMS provided by DB.
<br/>A popular external HMS is Amazon Glue.
<br/>This document describes the considerations for UCX integration with external HMS.

### Current Considerations
- Integration with external HMS is set up on individual clusters.
- Theoretically we can integrate separate clusters in a workspace with different HMS repositories.
- In reality most customers use a single (internal or external) HMS within a workspace.
- When migrating from an external HMS we have to consider that it is used by more than one workspace.
- Integration with external HMS has to be set on all DB Warehouses together.
- HMS connectivity is set, usually, on cluster policy. As well as global SQL Warehouse config
- Typically external HMS setup relies on:
  - Spark Config
  - Instance Profiles
  - Init scripts

### Design Decisions
- Should we set up a single HMS for UCX?
- We should suggest copying the setup from an existing Cluster/Cluster policy
- We shouldn't override the set up for the DB Warehouses (that may break functionality)
- We should allow overriding cluster settings and instance profile setting to accommodate novel settings.

### Challenges
- We cannot support multiple HMS
- Using an external HMS to persist UCX tables will break functionality for a second workspace using UCX
- We should consider using a pattern similar to our integration testing to rename the target database to allow persisting from multiple workspaces. For example WS1 --> UCX_ABC, WS2 --> UCX_DEF.
- With external HMS it is likely that some of the tables will not be accessible by some of the workspaces. We may need to migrate certain databases from certain workspaces.

### Suggested flow
1. Start the installer.
2. The installer looks for use of external HMS by the workspace. We review cluster policies or DBSQL warehouses settings.
3. We alert the user that an external HMS is set and request ask a YES/NO to set external HMS.
4. We alert the user if they opted for external HMS and the DB Warehouses are not set for external HMS
5. We update the configuration file with the HMS settings.
6. We set the job clusters with the required External HMS settings.
