---
height: 10
width: 2
---

## 2 - Group migration

The second step of succesfully adopting UC if migrating your workspace local groups to the account.
This step is a relatively low risk as it's an additive operation, it won't disturb your currently running pipelines.

Follow those steps in order to successfully migrate your groups to the account:

If you're not using an Identity Provider (Okta, Azure Entra etc...):
1. Create the groups at the account level, consider using the [create-account-groups](https://github.com/databrickslabs/ucx/blob/main/README.md#create-account-groups-command) command.
   1. For extra safety, consider running [validate-group-membership](https://github.com/databrickslabs/ucx/blob/main/README.md#validate-groups-membership-command) command to validate that you have the same amount of groups/users in the workspace and the account
   2. Enable SCIM at the Account level.

If you're using an Identity Provider:
1. Enable SCIM at the account level
2. Disable SCIM at the workspace level if not done already.
3. Trigger a sync from your IdP to the account
   1. To validate that all groups are properly setup for the group migration, run [validate-group-membership](https://github.com/databrickslabs/ucx/blob/main/README.md#validate-groups-membership-command)

Once the account groups are setup, perform the group migration by using the Group migration workflow, more information in the [docs](https://github.com/databrickslabs/ucx/blob/main/README.md#group-migration-workflow)
