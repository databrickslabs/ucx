---
height: 5
---

Workspace Group Migration
=========================

[Quick link to the group migration documentation.](https://github.com/databrickslabs/ucx/blob/main/docs/local-group-migration.md)

Prerequisites for group migration are:

 - The assessment workflow has completed.
 - For each workspace group a corresponding account-level group must exist. (Refer to the documentation for details about the different strategies that can be configured for associating workspace groups with their corresponding account-level group.)

Group migration performs the following steps:

1. Workspace groups are renamed to a temporary name.
2. The account-level groups are then provisioned into the workspace.
3. Permissions assigned to the workspace groups are replicated on the account-level groups.

These steps are performed by either the `migrate-groups` or the `migrate-groups-experimental` [workflows](/jobs). (The latter uses a faster, but experimental, method to replicate group permissions.)

Once group migration has taken place:

 - The `validate-groups-permissions` workflow can be used to check that permissions were correctly duplicated.
 - The `remove-workspace-local-backup-groups` workflow can be used to remove the (original) workspace-level groups.
