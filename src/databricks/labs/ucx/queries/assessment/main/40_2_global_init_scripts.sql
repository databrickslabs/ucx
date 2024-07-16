/* --title 'Incompatible Global Init Scripts' --width 3 --height 6 */
SELECT
  EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
  script_name,
  created_by
FROM inventory.global_init_scripts
ORDER BY
  script_name DESC