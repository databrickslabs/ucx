-- --title 'Global Init Scripts'
SELECT
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
    script_name,
    created_by
FROM
  inventory.global_init_scripts
ORDER BY script_name DESC
