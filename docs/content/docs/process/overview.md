---
title: Overview
linkTitle: Overview
weight: 1
---
On a high level, the steps in migration process are:

{{% steps %}}

### Assessment

Please follow the [assessment workflow](docs/reference/workflows/assessment.md) 

### Group Migration

Please follow the [group migration workflow](docs/reference/workflows/group_migration.md)

### Table Migration

Please follow the [table migration process](docs/process/table_migration.md)

### Post Migration Data Reconciliation

Please follow the [post-migration data reconciliation workflow](docs/reference/workflows/reconciliation.md)

### Code Migration

Please follow the [code migration commands](docs/reference/commands/code.md)

{{% /steps %}}


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