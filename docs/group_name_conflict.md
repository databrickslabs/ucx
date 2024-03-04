Group Name Conflict Resolution
===

See [this document](local-group-migration.md) for workspace group migration.

When migrating multiple workspaces we can run into conflicts.
These conflicts occur when groups with the same name in different workspaces have different membership and different
use.

During the installation process we pose the following question: `Do you need to rename the workspace groups to match the account groups' name?`

If the answer is "Yes" a follow-up question will be:

```text
Choose How to rename the workspace groups:
 [1] Apply a Prefix
 [2] Apply a Suffix
 [3] Use Regular Expression Substitution
 [4] User Regular Expression to extract a value from the account and the workspace
 [5] Map using External Group ID
```

The user then input the Prefix/Suffix/Regular Expression.
The installation process will validate the regular expression.
The installation process will register the selection as regular expression in the configuration YAML file.

We introduce 3 more parameters to the [configuration](../README.md#open-remote-config-command) and the group manager:

- workspace_group_regex
- workspace_group_replace
- account_group_regex

When we run the migration process the regular expression substitution will be applied on all groups.

Group Translation Scenarios:

| Scenario       | User Input                                                   | workspace_group_regex | workspace_group_replace | account_group_regex | Example                                          |
|----------------|--------------------------------------------------------------|-----------------------|-------------------------|---------------------|--------------------------------------------------|
| Prefix         | prefix: [Prefix]                                             | ^                     | [Prefix]                | [EMPTY]             | data_engineers --> prod_data_engineers           |
| Suffix         | suffix: [Prefix]                                             | $                     | [Suffix]                | [EMPTY]             | data_engineers --> data_engineers_prod           |
| Substitution   | Search Regex: [Regex]<br/>Replace Text:[Replacement_Text]    | [WS_Regex]            | [ [Replacement_Text]    | [Empty]             | corp_tech_data_engineers --> prod_data_engineers |
| Partial Lookup | Workspace Regex: [WS_Regex]<br/> Account Regex: [Acct Regex] | [WS_Regex]            | [Empty]                 | [Acct_Regex]        | data_engineers(12345) --> data_engs(12345)       |