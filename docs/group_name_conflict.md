# Group Name Conflict Resolution
During the UC upgrade process we migrate all the local workspace group to account level group.
The process is detailed here: [local-group-migration.md](local-group-migration.md)
<br/>
When migrating multiple workspaces we can run into conflicts. 
These conflicts occur when groups in different workspaces have different membership and different use.

## Suggested Workflow
During the installation process we pose the following question:
<br/>
"Do you need to rename the workspace groups to match the account groups' name?"


If the answer is "Yes" a follow up question will be:
<br/>
"Choose How to rename the workspace groups:"
1. Apply a Prefix
2. Apply a Suffix
3. Use Regular Expression
4. Return (Don't Rename Groups)

The user then input the Prefix/Suffix/Regular Expression.
The install process will validate the regular expression.
The install process will register the selection as regular expression in the configuration YAML file.

When we run the migration process the regular expression substitution will be applied on all groups.
