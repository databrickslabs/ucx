/* --title 'Incompatible Delta Live Tables' --width 3 --height 6 */
SELECT
  EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding,
  pipeline_name,
  creator_name
FROM inventory.pipelines
ORDER BY
  pipeline_name DESC