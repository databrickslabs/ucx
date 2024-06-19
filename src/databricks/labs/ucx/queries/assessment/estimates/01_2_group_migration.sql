-- --title 'Workspace local groups'
-- --title Workspace local groups to migrate, row=1, col=2, size_x=3, size_y=8
SELECT
    id_in_workspace,
    name_in_workspace,
    name_in_account,
    temporary_name,members,
    entitlements,
    external_id,
    roles
FROM inventory.groups
