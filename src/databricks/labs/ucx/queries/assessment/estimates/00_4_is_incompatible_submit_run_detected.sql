/* --title 'Incompatible submit runs detected' --height 10 --width 4 */
SELECT
  object_type,
  object_id,
  finding
FROM (
  SELECT
    object_type,
    object_id,
    EXPLODE(FROM_JSON(failures, 'array<string>')) AS finding
  FROM inventory.objects
)
WHERE
  finding = 'no data security mode specified' AND object_type = 'submit_runs'