---
title: "Process"
linkTitle: "Process"
weight: 2
---
On a high level, the steps in migration process are:
1. [assessment](#assessment-workflow)
2. [group migration](#group-migration-workflow)
3. [table migration](#table-migration-process)
4. [data reconciliation](#post-migration-data-reconciliation-workflow)
5. [code migration](#code-migration-commands)

The migration process can be schematic visualized as:

```mermaid
flowchart TD
    subgraph workspace-admin
        assessment --> group-migration
        group-migration --> table-migration
        table-migration --> code-migration
        assessment --> create-table-mapping
        create-table-mapping --> table-migration
        create-table-mapping --> code-migration
        validate-external-locations --> table-migration
        assessment --> validate-table-locations
        validate-table-locations --> table-migration
        table-migration --> revert-migrated-tables
        revert-migrated-tables --> table-migration
    end
    subgraph account-admin
        create-account-groups --> group-migration
        sync-workspace-info --> create-table-mapping
        group-migration --> validate-groups-membership
    end
    subgraph iam-admin
        setup-account-scim --> create-account-groups
        assessment --> create-uber-principal
        create-uber-principal --> table-migration
        assessment --> principal-prefix-access
        principal-prefix-access --> migrate-credentials
        migrate-credentials --> validate-external-locations
        setup-account-scim
    end
```