-- viz type=table, name=Global Init Scripts, columns=finding,script_name,created_by
-- widget title=Incompatible Global Init Scripts, row=42, col=3, size_x=3, size_y=8
SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
    script_name,
    created_by
FROM
  $inventory.global_init_scripts
ORDER BY script_name DESC