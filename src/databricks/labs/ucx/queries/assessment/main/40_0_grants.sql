/* --title 'Incompatible Object Privileges' --width 6 */
SELECT
  EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
  action_type,
  object_type,
  object_id,
  principal,
  principal_type
FROM inventory.grant_detail
WHERE
  STARTSWITH(action_type, 'DENIED_')
ORDER BY
  object_id,
  object_type,
  action_type,
  principal,
  principal_type