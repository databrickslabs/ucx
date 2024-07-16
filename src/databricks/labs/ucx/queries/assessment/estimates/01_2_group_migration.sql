/* --title 'Workspace local groups' --height 10 --width 3 */
SELECT
  id_in_workspace,
  name_in_workspace,
  name_in_account,
  temporary_name,
  members,
  entitlements,
  external_id,
  roles
FROM inventory.groups