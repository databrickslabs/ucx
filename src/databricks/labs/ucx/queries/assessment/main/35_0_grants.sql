-- viz type=table, name=Grants, columns=finding, principal,action_type,object_type,object_id,principal
-- widget title=Incompatible Object Privileges, row=41, col=0, size_x=6, size_y=8
SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
    action_type,
    object_type,
    object_id,
    principal,
    principal_type
FROM $inventory.grant_detail
WHERE startswith(action_type, 'DENIED_')
ORDER BY
    object_id, object_type, action_type, principal, principal_type
